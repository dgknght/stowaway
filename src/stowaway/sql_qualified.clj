(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.set :refer [union]]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [ubergraph.core :as g]
            [ubergraph.alg :as ga]
            [stowaway.sql :as sql]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [camel-snake-kebab.core :refer [->snake_case]]))

(s/def ::relationship (s/tuple keyword? keyword?))
(s/def ::relationships (s/coll-of ::relationship :min-count 1))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.PersistentHashMap ::map)

(def apply-limit sql/apply-limit)
(def apply-offset sql/apply-offset)
(def select-count sql/select-count)
(def plural sql/plural)
(def delimit sql/delimit)

(defn- apply-word-rule
  [word {:keys [pattern f]}]
  (when-let [match (re-find pattern word)]
    (f match)))

(defn- singular
  [word]
  (some (partial apply-word-rule word)
        [{:pattern #"(?i)\Achildren\z"
          :f (constantly "child")}
         {:pattern #"\A(.+)ies\z"
          :f #(str (second %) "y")}
         {:pattern #"\A(.+)s\z"
          :f #(str (second %))}]))

(defn- postgres-array
  [values]
  (format "'{%s}'"
          (->> values
               (map delimit)
               (str/join ","))))

(defn- model->table
  [m {:keys [table-names]
      :or {table-names {}}}]
  {:pre [(keyword? m)]}
  (get-in table-names [m] (-> m name plural ->snake_case keyword)))

(defn- model->table-key
  [m opts]
  (keyword (model->table m opts)))

(defn- ->col-ref
  "Accepts a qualified keyword and returns a column reference
  in the form of table_name.column_name"
  [k opts]
  (let [field (name k)
        model (namespace k)
        table-name (model->table (keyword model) opts)]
    (keyword (->> [(name table-name) field]
                  (filter identity)
                  (map ->snake_case)
                  (str/join ".")))))

(defn- normalize-sort-spec
  [sort-spec]
  (if (vector? sort-spec)
    sort-spec
    [sort-spec :asc]))

(defn apply-sort
  [sql {:keys [sort] :as opts}]
  (if sort
    (apply h/order-by sql (map (comp (fn [s]
                                       (update-in s [0] #(->col-ref % opts)))
                                     normalize-sort-spec)
                               sort))
    sql))

(defmulti ^:private map-entry->statements
  (fn [[_k v]]
    (type v)))

(defmethod map-entry->statements :default
  [[k v]]
  [[:= k v]])

(defmethod map-entry->statements ::vector
  [[k [oper & [v1 v2 :as values]]]]
  (case oper

      (:= :> :>= :<= :< :<> :!= :like)
      [[oper k v1]]

      :between
      [[:>= k v1]
       [:<= k v2]]

      :between>
      [[:>= k v1]
       [:< k v2]]

      :<between
      [[:> k v1]
       [:<= k v2]]

      :<between>
      [[:> k v1]
       [:< k v2]]

      :in
      [(apply vector :in k values)]

      (:and :or)
      [(apply vector oper (->> values
                               (interleave (repeat k))
                               (partition 2)
                               (mapcat map-entry->statements)))]

      :contained-by
      [[(keyword "@>") v1 k]]

      :any
      [[:= v1 [:any k]]]

      :&&
      [[oper (postgres-array v1) k]]

      [(apply vector :in k values)]))

(defmulti ^:private ->clauses
  (fn [criteria _opts]
    (type criteria)))

(defmethod ->clauses ::map
  [criteria opts]
  (->> criteria
       (map (fn [e]
              (update-in e [0] #(->col-ref % opts))))
       (mapcat map-entry->statements)
       seq))

(declare ->where)

(defmethod ->clauses ::vector
  [[oper & criterias] opts]
  [(apply vector oper (map #(->where % opts) criterias))])

(defn ->where
  [criteria opts]
  (when-let [clauses (->clauses criteria opts)]
    (if (= 1 (count clauses))
      (first clauses)
      (apply vector :and clauses))))

(defmulti ^:private namespaces type)

(defmethod namespaces ::map
  [m]
  (->> (keys m)
       (map namespace)
       (filter identity)
       (into #{})))

(defmethod namespaces ::vector
  [[_oper & criterias]]
  (->> criterias
       (map namespaces)
       (reduce union)))

(defn- single-ns
  "Give a map, returns the single namepace used in all of the keys,
  or returns nil if multiple namespaces are used."
  [criteria]
  (let [ns (namespaces criteria)]
    (when (= 1 (count ns))
      (first ns))))

(defn- path-to-join
  [path _relationships] ; we will want to support explicit join details
  (->> path
       (partition 2 1)
       (map (fn [[t1 t2]]
              [t2 [:=
                   (keyword (str (name t1) ".id"))
                   (keyword (str (name t2)
                                 "."
                                 (singular (name t1))
                                 "_id"))]]))))

(defn- shortest-path
  [graph from to]
  (ga/nodes-in-path
    (ga/shortest-path graph from to)))

(defn ->joins
  [criteria {:keys [table relationships] :as opts}]
  {:pre [(or (nil? relationships)
             (s/valid? ::relationships relationships))]}

  (when (seq relationships)
    (let [graph (apply g/graph relationships)]
      (->> (namespaces criteria)
           (map (comp #(shortest-path graph table %)
                      #(model->table-key % opts)
                      keyword))
           (sort-by count >)
           (reduce (fn [ps p]
                     (if (some #(= (take (count p) %)
                                   p)
                               ps)
                       ps
                       (conj ps p)))
                   [])
           ; redundant paths removed
           (mapcat #(path-to-join % relationships))
           ; TODO: Dedupe here?
           (mapcat identity)))))

(defn- join
  [sql joins]
  (if (seq joins)
    (assoc sql :join joins)
    sql))

(defn ->query
  [criteria & [{:keys [target] :as opts}]]
  (let [target (or target
                   (keyword (single-ns criteria))
                   (throw (IllegalArgumentException. "No target specified.")))
        table (model->table target opts)]
    (-> (h/select (keyword (str (name table) ".*")))
        (h/from table)
        (h/where (->where criteria opts))
        (join (->joins criteria (assoc opts :table table)))
        (apply-sort opts)
        (apply-limit opts)
        (apply-offset opts)
        (select-count opts)
        (hsql/format))))
