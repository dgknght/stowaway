(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.set :refer [union]]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.graph :as g]
            [stowaway.sql :as sql]))

(s/def ::relationship (s/tuple keyword? keyword?))
(s/def ::relationships (s/coll-of ::relationship :min-count 1))
(s/def ::joins (s/map-of ::relationship vector?))

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

(defn- select-custom
  [sql {:keys [select] :as opts}]
  (if (seq select)
    (let [cols (map #(->col-ref % opts) select)]
      (-> sql
          (dissoc :select)
          (assoc :select cols)))
    sql))

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

(defn- subquery?
  [expr]
  (and (vector? expr)
       (= :in (first expr))
       (list? (second expr))))

(defmulti ^:private map-entry->statements
  (fn [[_k v]]
    (if (subquery? v)
      ::subquery
      (type v))))

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

(declare ->query)

(defmethod map-entry->statements ::subquery
  [[k [_ [criteria opts]]]]
  [[:in k (->query criteria (assoc opts :skip-format? true))]])

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

(defn- k-join
  [& vs]
  (keyword
    (->> vs
         (map #(if (keyword? %)
                 (name %)
                 %))
         (str/join))))

(defn- find-relationship
  "Give two table names in any order, return the relationship
  which contains them in primary first, foreign second order."
  [edge {:keys [relationships]}]
  (some relationships [edge (reverse edge)]))

(defn- ->join
  "Given an edge (two tables in any order), return the honeysql join clause."
  [edge {:keys [joins]
         :or {joins {}}
         :as opts}]
  {:pre [(or (nil? joins)
             (s/valid? ::joins joins))]}
  (let [[t1 t2 :as rel] (find-relationship edge opts)]
    (or (joins rel)
        [:=
         (k-join t1 ".id")
         (k-join t2 "." (-> t1 name singular) "_id")])))

(defn- join-type
  [t1 t2 {:keys [full-results]}]
  (if (full-results t1)
    (if (full-results t2)
      :outer
      :left)
    (if (full-results t2)
      :right
      :inner)))

(defn- path-to-join
  "Given a sequence of table names, return the join clauses necessary
  to include all the tables in the query."
  [path opts]
  (->> path
       (partition 2 1)
       (map (fn [[t1 t2 :as rel]]
              [(join-type t1 t2 opts)
               [t2 (->join rel opts)]]))))

(defn- extract-tables
  [criteria opts]
  (map (comp #(model->table-key % opts)
             keyword)
       (namespaces criteria)))

(defn ->joins
  "Given a criteria map, return a sequence of join clauses that
  includes all tables in the criteria, plus the target table."
  [criteria {:keys [table relationships] :as opts}]
  {:pre [(or (nil? relationships)
             (s/valid? ::relationships relationships))]}
  (when (seq relationships)
    (->> (g/shortest-paths table
                           (extract-tables criteria opts)
                           relationships)
         (mapcat #(path-to-join % opts))
         (reduce (fn [res [join-type join]]
                   (update-in res [join-type] (fnil into []) join))
                 {}))))

(defn- join
  "Append left join, right join, full outer join, and inner
  join clauses to a SQL map"
  [sql {:keys [left right inner outer]}]
  (cond-> sql
    (seq left) (assoc :left-join left)
    (seq right) (assoc :right-join right)
    (seq outer) (assoc :outer-join outer)
    (seq inner) (assoc :join inner)))

(defn ->query
  "Translate a criteria map into a SQL query"
  [criteria & [{:keys [target named-params skip-format?] :as opts}]]
  (let [target (or target
                   (keyword (single-ns criteria))
                   (throw (IllegalArgumentException. "Unable to determine the query target")))
        table (model->table target opts)
        fmt (if skip-format?
              identity
              #(hsql/format % {:params named-params}))]
    (-> (h/select (k-join table ".*"))
        (h/from table)
        (h/where (->where criteria opts))
        (join (->joins criteria
                       (-> opts
                           (assoc :table table)
                           (update-in [:full-results]
                                      (fn [models]
                                        (->> models
                                             (map #(model->table-key % opts))
                                             (into #{})))))))
        (apply-sort opts)
        (apply-limit opts)
        (apply-offset opts)
        (select-custom opts)
        (select-count opts)
        fmt)))
