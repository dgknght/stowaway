(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.util :refer [key-join]]
            [stowaway.inflection :refer [plural
                                         singular]]
            [stowaway.graph :as g]
            [stowaway.core :as stow]
            [stowaway.criteria :as c :refer [namespaces
                                             single-ns]]
            [stowaway.sql :as sql]))

(s/def ::relationship (s/tuple keyword? keyword?))
(s/def ::relationships (s/coll-of ::relationship :min-count 1))
(s/def ::joins (s/map-of ::relationship vector?))

(def apply-limit sql/apply-limit)
(def apply-offset sql/apply-offset)
(def select-count sql/select-count)
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

(declare ->query)

(defmethod map-entry->statements ::stow/vector
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

    :including
    [[:in (keyword (namespace k) "id")
      (assoc (->query v1 {:skip-format? true})
             :select [(keyword (plural (single-ns v1))
                               (str (singular (first (str/split (name k) #"\."))) "_id"))])]]

    [(apply vector :in k values)]))

(defmethod map-entry->statements ::subquery
  [[k [_ [criteria opts]]]]
  [[:in k (->query criteria (assoc opts :skip-format? true))]])

(defmulti ^:private ->clauses
  (fn [criteria _opts]
    (type criteria)))

(defn- id-key?
  [k]
  (= "id" (name k)))

(defn- normalize-model-ref
  [[_ v :as e]]
  (if (and (map? v)
           (:id v))
    (-> e
        (update-in [0] #(keyword (namespace %) (str (name %) "_id")))
        (update-in [1] :id))
    e))

(defn- coerce-id-value
  [[k :as e] {:keys [coerce-id]
              :or {coerce-id identity}}]
  (if (id-key? k)
    (update-in e [1] coerce-id)
    e))

(defn- normalize-col-ref
  [e opts]
  (update-in e [0] #(->col-ref % opts)))

(defmethod ->clauses ::stow/map
  [criteria opts]
  (->> criteria
       (map (comp #(normalize-col-ref % opts)
                  #(coerce-id-value % opts)
                  normalize-model-ref))
       (mapcat map-entry->statements)
       seq))

(declare ->where)

(defmethod ->clauses ::stow/vector
  [[oper & criterias] opts]
  [(apply vector oper (map #(->where % opts) criterias))])

(defn ->where
  [criteria opts]
  (when-let [clauses (->clauses criteria opts)]
    (if (= 1 (count clauses))
      (first clauses)
      (apply vector :and clauses))))

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
         (key-join t1 ".id")
         (key-join t2 "." (-> t1 name singular) "_id")])))

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
                           :relationships relationships)
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
  {:pre [(s/valid? ::c/criteria criteria)]}
  (let [target (or target
                   (keyword (single-ns criteria))
                   (throw (IllegalArgumentException. "Unable to determine the query target")))
        table (model->table target opts)
        fmt (if skip-format?
              identity
              #(hsql/format % {:params named-params}))]
    (-> (h/select (key-join table ".*"))
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
