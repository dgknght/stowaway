(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.set :refer [union]]
            [clojure.pprint :refer [pprint]]
            [stowaway.sql :as sql]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [camel-snake-kebab.core :refer [->snake_case]]))

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
  (get-in table-names [m] (-> m name plural)))

(defn- ->col-ref
  "Accepts a qualified keyword and returns a column reference
  in the form of table_name.column_name"
  [k opts]
  (let [field (name k)
        model (namespace k)
        table-name (model->table (keyword model) opts)]
    (keyword (->> [table-name field]
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

(defn ->where
  [criteria opts]
  (when-let [clauses (->> criteria
                     (map (fn [e]
                            (update-in e [0] #(->col-ref % opts))))
                     (mapcat map-entry->statements)
                     seq)]
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

(defn ->query
  [criteria & [{:keys [target] :as opts}]]
  (let [target (or (keyword (single-ns criteria))
                   target
                   (throw (IllegalArgumentException. "No target specified.")))
        table (model->table target opts)]
    (-> (h/select (keyword (str table ".*")))
        (h/from (keyword table))
        (h/where (->where criteria opts))
        (apply-sort opts)
        (apply-limit opts)
        (apply-offset opts)
        (select-count opts)
        (hsql/format))))
