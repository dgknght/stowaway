(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.set :refer [union]]
            [clojure.pprint :refer [pprint]]
            [stowaway.sql :as sql]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [camel-snake-kebab.core :refer [->snake_case]]
            [clojure.pprint :as pp]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.PersistentHashMap ::map)

(def apply-limit sql/apply-limit)
(def apply-offset sql/apply-offset)
(def select-count sql/select-count)
(def plural sql/plural)
(def delimit sql/delimit)

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
  (get-in table-names [m] (-> m name plural keyword)))

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
      [[:in k values]]

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

      [[:in k values]]))

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

(defn ->joins
  [criteria {:keys [table relationships]
             :or {relationships {}}
             :as opts}]
  (let [tables (map (comp #(model->table % opts)
                          keyword)
                    (namespaces criteria))]
    (when (seq tables)
      (->> tables
           (remove #(= table %))
           (mapcat (comp (fn [[rel-key join-exp]]
                           [(first (disj rel-key table))
                            join-exp])
                         (juxt identity relationships)
                         (fn [t] #{table t})))))))

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
