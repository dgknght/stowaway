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
  [m {:keys [table-names pluralize?]
      :or {table-names {}}}]
  {:pre [(keyword? m)]}
  (let [plurality (if pluralize? plural identity)]
    (get-in table-names [m] (-> m name plurality ->snake_case keyword))))

(defn- model->table-key
  [m opts]
  (keyword (model->table m opts)))

(defn- ->col-ref
  "Accepts a qualified keyword and returns a column reference
  in the form of table_name.column_name"
  [k {:keys [target] :as opts}]
  (let [field (name k)
        model (or (namespace k)
                  target)
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

(defn- ->seq
  [x]
  (when x
    (if (sequential? x)
      x
      [x])))

(defn- pluralize-namespace
  [k]
  (if-let [n (namespace k)]
    (keyword (plural n)
           (name k))
    k))

(defn build-select
  "Construct the select clause.

  By default, this is <table>.*.

  You can specified :select, which will replace the default, or
  :select-also, which will supplement the default."
  [table {:keys [select-also select pluralize?]}]
  (let [plurality (if pluralize? pluralize-namespace identity)]
    (if select
      (map plurality
           (->seq select))
      (cons (key-join table ".*")
            (map plurality (->seq select-also))))))

(defn- subquery?
  [expr]
  (and (vector? expr)
       (= :in (first expr))
       (list? (second expr))))

(defmulti ^:private map-entry->statements
  (fn [[_k v] _opts]
    (if (subquery? v)
      ::subquery
      (type v))))

(defmethod map-entry->statements :default
  [[k v] _opts]
  [[:= k v]])

(declare ->query)

(defn- naked-kw->string
  [k]
  (if (sequential? k)
    (map naked-kw->string k)
    (if (and (keyword? k)
             (not (namespace k)))
      (name k)
      k)))

; For keywords, if they have a namespace, assument it is a
; column reference. Otherwise, assume it's a value and should
; be converted to a string
(defmethod map-entry->statements ::stow/keyword
  [[k v] _opts]
  [[:= k (naked-kw->string v) ]])

(defmethod map-entry->statements ::stow/vector
  [[k [oper & [v1 v2 :as values]]] {:keys [pluralize?] :as opts}]
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
    [(apply vector :in k (map naked-kw->string values))]

    (:and :or)
    [(apply vector oper (->> values
                             (interleave (repeat k))
                             (partition 2)
                             (mapcat #(map-entry->statements % opts))))]

    :contained-by
    [[(keyword "@>") v1 k]]

    :any
    [[:= v1 [:any k]]]

    :&&
    [[oper (postgres-array v1) k]]

    :including
    [[:in (keyword (namespace k) "id")
      (let [[plurality singularity] (if pluralize?
                                      [plural singular]
                                      [identity identity])]
        (assoc (->query v1 {:skip-format? true})
               :select [(keyword (plurality (single-ns v1))
                                 (str (singularity (first (str/split (name k) #"\."))) "_id"))]))]]

    [(apply vector :in k values)]))

(defmethod map-entry->statements ::subquery
  [[k [_ [criteria opts]]] _opts]
  [[:in k (->query criteria (assoc opts :skip-format? true))]])

(defmulti ^:private ->clauses
  (fn [criteria _opts]
    (type criteria)))

(defn- id-key?
  [k]
  (= "id" (name k)))

(defn- model-ref?
  [x]
  (if (sequential? x)
    (some model-ref? x)
    (and (map? x)
       (contains? x :id))))

(defn- deref-model
  [x]
  (cond
    (sequential? x) (mapv deref-model x)
    (keyword? x) x
    :else (:id x)))

(defn- normalize-model-ref
  [[_ v :as e]]
  (if (model-ref? v)
    (-> e
        (update-in [0] #(keyword (namespace %) (str (name %) "_id")))
        (update-in [1] deref-model))
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
       (mapcat #(map-entry->statements % opts))
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
  [edge {:keys [joins aliases pluralize?]
         :or {joins {}
              aliases {}}
         :as opts}]
  {:pre [(or (nil? joins)
             (s/valid? ::joins joins))]}
  (let [[t1 t2 :as rel] (find-relationship edge opts)
        singularity (if pluralize? singular identity)]
    (or (joins rel)
        [:=
         (key-join (get-in aliases [t1] t1) ".id")
         (key-join (get-in aliases [t2] t2) "." (-> t1 name singularity) "_id")])))

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

(defn- option-namespaces
  [{:keys [select-also]}]
  (when select-also
    (map namespace (->seq select-also))))

(defn- extract-tables
  [criteria opts]
  (->> (namespaces criteria)
       (concat (option-namespaces opts))
       set
       (map (comp #(model->table-key % opts)
                  keyword))))

(defn ->joins
  "Given a criteria map, return a sequence of join clauses that
  includes all tables in the criteria, plus the target table."
  [criteria {:keys [table relationships] :as opts}]
  {:pre [(or (nil? relationships)
             (s/valid? ::relationships relationships))]}
  (when (seq relationships)
    ; Putting the values in a map eliminates duplicates.
    ; We may want to reverse the order and make the table
    ; the outer key and the type of join the inner key,
    ; as duplicates are still possible the way it's written if
    ; the same table is listed with two different join types
    (let [mapped (->> (g/shortest-paths table
                                        (extract-tables criteria opts)
                                        :relationships relationships)
                      (mapcat #(path-to-join % opts))
                      (reduce (fn [mapped [join-type [table exp]]]
                                (update-in mapped
                                           [join-type]
                                           (fnil assoc {}) table exp))
                              {}))]
      (update-vals mapped (fn [join-map]
                            (mapcat identity (seq join-map)))))))

(defn- join
  "Append left join, right join, full outer join, and inner
  join clauses to a SQL map"
  [sql {:keys [left right inner outer]}]
  (cond-> sql
    (seq left) (assoc :left-join left)
    (seq right) (assoc :right-join right)
    (seq outer) (assoc :outer-join outer)
    (seq inner) (assoc :join inner)))

(defn- simple-query
  [criteria table opts]
  (-> {:select (build-select table opts)}
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
      (select-count opts)))


; I can't think of a good name for the recrusion keys,
; so I'm just addressing them by position for now.
(defn- recursive-query
  [criteria table {[k1 k2] :recursion :as opts}]
  (-> (h/with-recursive
        [:cte (h/union (simple-query criteria table opts)
                       (-> {:select (build-select table opts)}
                           (h/from table)
                           (h/join :cte [:= (key-join table "." k1) (key-join :cte "." k2)])))])
      (h/select :cte.*)
      (h/from :cte)))

(defn ->query
  "Translate a criteria map into a SQL query

  Note that when map values are keywords, some assumptions are made.

  If a keyword has a namespace, it is assumed to be a column reference. A keyword
  without a namespace is assumed to be a value that must be converted to a string."
  [criteria & [{:keys [target named-params skip-format? quoted?] :as opts}]]
  {:pre [(s/valid? ::c/criteria criteria)]}
  (let [target (or target
                   (keyword (single-ns criteria))
                   (throw (IllegalArgumentException. "Unable to determine the query target")))
        table (model->table target opts)
        fmt (if skip-format?
              identity
              #(hsql/format % {:params named-params
                               :quoted quoted?}))]
    (fmt (if (:recursion opts)
           (recursive-query criteria table opts)
           (simple-query criteria table opts)))))

(defn- join-update
  [sql table alias joins]
  (if (seq joins)
    (-> sql
        (assoc :from [[table alias]])
        (join joins))
    sql))

(defn ->update
  [changes criteria & {:as opts :keys [target]}]
  (let [target (or target
                   (keyword (single-ns changes))
                   (throw (IllegalArgumentException. "Unable to determine the update target")))
        table (model->table target opts)
        joins (->joins criteria
                       (-> opts
                           (assoc :table table
                                  :aliases {table :x})
                           (update-in [:full-results]
                                      (fn [models]
                                        (->> models
                                             (map #(model->table-key % opts))
                                             (into #{}))))))]
    (hsql/format
      (cond-> {:update table
               :set changes
               :where (->where
                        criteria
                        opts)}
        (seq joins) (join-update table 'x joins)))))
