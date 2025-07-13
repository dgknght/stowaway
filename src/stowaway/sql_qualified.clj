(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.set :refer [difference]]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [honey.sql.pg-ops :as ops]
            [stowaway.util :refer [key-join]]
            [stowaway.graph :as g]
            [stowaway.core :as stow]
            [stowaway.criteria :as c :refer [namespaces
                                             single-ns]]
            [stowaway.sql :as sql]))

(s/def ::relationship (s/tuple keyword? keyword?))
(s/def ::relationships (s/coll-of ::relationship :min-count 1 :kind set?))
(s/def ::column (s/or :same keyword?
                      :different (s/tuple any? any?))) ; TODO: This should be any scalar value, I think
(s/def ::columns (s/coll-of ::column))
(s/def ::joins (s/map-of ::relationship ::columns))

(def apply-limit sql/apply-limit)
(def apply-offset sql/apply-offset)
(def select-count sql/select-count)

(defn- pg-array
  [[v :as vs] type]
  (cond-> [:array
           (cond
             (keyword? v)
             (mapv name vs)

             :else
             (vec vs))]
    type (conj type)))

(def ^:private split-kw (juxt namespace name))

(defn- ->col-ref
  "Accepts a qualified keyword and returns a column reference
  in the form of table_name.column_name"
  ([opts]
   #(->col-ref % opts))
  ([k {:keys [table
              model->table
              column-fn]}]
   (let [[ns n] (split-kw k)]
     (keyword (if ns
                (model->table ns)
                (name table))
              (column-fn n)))))

(defn- normalize-sort-spec
  [sort-spec]
  (if (vector? sort-spec)
    sort-spec
    [sort-spec :asc]))

(defn apply-sort
  [sql {:keys [sort] :as opts}]
  (if sort
    (apply h/order-by sql (map (comp (fn [s]
                                       (update-in s [0] (->col-ref opts)))
                                     normalize-sort-spec)
                               sort))
    sql))

(defn- ->seq
  [x]
  (when x
    (if (sequential? x)
      x
      [x])))

(defn- update-keyword
  ([ns-f name-f]
   #(update-keyword % ns-f name-f))
  ([k ns-f name-f]
   (let [[ns n] ((juxt namespace name) k)]
     (if ns
       (keyword (ns-f ns)
                (name-f n))
       (name-f k)))))

(defn build-select
  "Construct the select clause.

  By default, this is <table>.*.

  You can specified :select, which will replace the default, or
  :select-also, which will supplement the default."
  [{:keys [select-also
           select
           table-fn
           column-fn
           table]}]
  (if select
    (map (update-keyword table-fn column-fn)
         (->seq select))
    (cons (keyword (name table) "*")
          (map (update-keyword table-fn column-fn)
               (->seq select-also)))))

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
  [[k [oper & [v1 v2 :as values]]] {:keys [table-fn] :or {table-fn identity} :as opts}]
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
    [['&& (-> v1 seq (pg-array v2)) k]]

    :including
    [[:in (keyword (namespace k) "id")
      (assoc (->query v1 {:skip-format? true})
             :select [(keyword (table-fn (single-ns v1))
                               (str (namespace k) "_id"))])]]

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
  (update-in e [0] (->col-ref opts)))

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
  [edge {:keys [joins
                aliases
                model->table
                column-fn]
         :or {joins {}
              aliases {}
              column-fn identity}
         :as opts}]
  {:pre [(or (nil? joins)
               (s/valid? ::joins joins))]}
  (let [[t1 t2 :as rel] (find-relationship edge opts)
        columns (or (joins rel)
                    [[:id (keyword (str (name t1) "-id"))]])
        clauses (map (fn [c]
                       (let [[c1 c2] (if (coll? c)
                                       c
                                       [c c])]
                         [:=
                          (if (keyword? c1)
                            (keyword (name (model->table (aliases t1 t1)))
                                     (name (column-fn c1)))
                            c1)
                          (if (keyword? c2)
                            (keyword (name (model->table (aliases t2 t2)))
                                     (name (column-fn c2)))
                            c2)]))
                     columns)]
    (if (= 1 (count clauses))
      (first clauses)
      (apply vector :and clauses))))

(defn- join-type
  [t1 t2 {:keys [full-results]}]
  (if (full-results t1)
    (if (full-results t2)
      :outer
      :left)
    (if (full-results t2)
      :right
      :inner)))

(defn- path->join
  "Given a sequence of table names, return the join clauses necessary
  to include all the tables in the query."
  [path {:as opts :keys [model->table]}]
  (->> path
       (partition 2 1)
       (map (fn [[t1 t2 :as rel]]
              [(join-type t1 t2 opts)
               [(model->table t2) (->join rel opts)]]))))

(defn- option-namespaces
  [{:keys [select-also]}]
  (when select-also
    (map namespace (->seq select-also))))

(defn- extract-targets
  [criteria {:as opts :keys [target]}]
  (let [unique-spaces (->> (namespaces criteria)
                           (concat (option-namespaces opts))
                           set)]
    (map keyword
         (difference unique-spaces
                     #{(name target)}))))

(defn ->joins
  "Given a criteria map, return a sequence of join clauses that
  includes all tables in the criteria, plus the target table."
  [criteria {:keys [target relationships] :as opts}]
  {:pre [(or (nil? relationships)
             (s/valid? ::relationships relationships))]}
  (let [targets (extract-targets criteria opts)]
    (when (seq targets)
      (when-not (seq relationships)
        (throw (ex-info "Multiple tables were specified, but no relationships were specified."
                        {:criteria criteria
                         :tables targets})))
      ; Putting the values in a map eliminates duplicates.
      ; We may want to reverse the order and make the table
      ; the outer key and the type of join the inner key,
      ; as duplicates are still possible the way it's written if
      ; the same table is listed with two different join types
      (let [mapped (->> (g/shortest-paths target
                                          targets
                                          :relationships relationships)
                        (mapcat #(path->join % opts))
                        (reduce (fn [mapped [join-type [table exp]]]
                                  (update-in mapped
                                             [join-type]
                                             (fnil assoc {}) table exp))
                                {}))]
        (update-vals mapped (fn [join-map]
                              (mapcat identity (seq join-map))))))))

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
  [criteria {:keys [table model->table] :as opts}]
  (-> {:select (build-select opts)}
      (h/from table)
      (h/where (->where criteria opts))
      (join (->joins criteria
                     (-> opts
                         (assoc :table table)
                         (update-in [:full-results]
                                    (fn [models]
                                      (->> models
                                           (map model->table)
                                           (into #{})))))))
      (apply-sort opts)
      (apply-limit opts)
      (apply-offset opts)
      (select-count opts)))


; I can't think of a good name for the recrusion keys,
; so I'm just addressing them by position for now.
(defn- recursive-query
  [criteria {[k1 k2] :recursion :as opts :keys [table]}]
  (-> (h/with-recursive
        [:cte (h/union (simple-query criteria opts)
                       (-> {:select (build-select opts)}
                           (h/from table)
                           (h/join :cte [:= (key-join table "." k1) (key-join :cte "." k2)])))])
      (h/select :cte.*)
      (h/from :cte)))

(defn- model->table*
  [{:keys [table-fn table-names]
    :or {table-names {}}}]
  (fn [model]
    {:pre [(or (keyword? model) (string? model))]}
    (let [k (if (string? model)
              (keyword model)
              model)
          result (table-names k (table-fn model))]
      (if (string? model)
        (name result)
        result))))

(defn- +model->table
  [opts]
  (assoc opts :model->table (model->table* opts)))

(defn- ensure-fns
  [opts]
  (-> opts
      (update-in [:column-fn] (fnil identity identity))
      (update-in [:table-fn] (fnil identity identity))))

(defn- +table
  [{:as opts :keys [target model->table]}]
  (assoc opts :table (model->table target)))

(defn- ensure-target
  [{:as opts :keys [target]} infered-ns]
  (let [target (or target
                   (keyword infered-ns)
                   (throw (IllegalArgumentException. "Unable to determine the query target")))]
    (assoc opts :target target)))

(defn- refine-opts
  [opts infered-ns]
  (-> opts
      (ensure-target infered-ns)
      ensure-fns
      +model->table
      +table))

(defn ->query
  "Translate a criteria map into a SQL query

  Note that when map values are keywords, some assumptions are made.

  If a keyword has a namespace, it is assumed to be a column reference. A keyword
  without a namespace is assumed to be a value that must be converted to a string."
  [criteria & [{:keys [named-params skip-format? quoted?] :as options}]]
  {:pre [(s/valid? ::c/criteria criteria)]}
  (let [opts (refine-opts options
                          (-> criteria single-ns keyword))
        fmt (if skip-format?
              identity
              #(hsql/format % {:params named-params
                               :quoted quoted?}))
        ->query (if (:recursion opts)
                  recursive-query
                  simple-query)]
    (fmt (->query criteria opts))))

(defn- join-update
  [sql table alias joins]
  (if (seq joins)
    (-> sql
        (assoc :from [[table alias]])
        (join joins))
    sql))

(defn ->update
  [changes criteria & options]
  (let [{:keys [table target model->table]
         :as opts} (refine-opts options
                                (-> changes single-ns keyword))
        joins (->joins criteria
                       (-> opts
                           (assoc :aliases {target :x})
                           (update-in [:full-results]
                                      (fn [models]
                                        (->> models
                                             (map model->table)
                                             (into #{}))))))]
    (hsql/format
      (cond-> {:update table
               :set changes
               :where (->where
                        criteria
                        opts)}
        (seq joins) (join-update table 'x joins)))))
