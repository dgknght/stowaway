(ns stowaway.datalog
  (:require [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]]
            [clojure.set :refer [difference]]
            [clojure.walk :refer [prewalk]]
            [ubergraph.core :as uber]
            [ubergraph.alg :refer [shortest-path
                                   nodes-in-path]]
            [stowaway.core :as stow]
            [stowaway.util :refer [type-dispatch]]
            [stowaway.inflection :refer [singular]]
            [stowaway.criteria :as c]
            [stowaway.graph :as g]))

(defn- parse-id
  [x]
  (if (string? x)
    (parse-long x)
    x))

(def ^:private conj*
  (fnil conj []))

(defn- dispatch-criterion
  [c & _]
  {:pre [(vector? c)]}
  (let [[_k v] c]
    (cond
      (map? v)    :model-ref
      (vector? v) (case (first v)
                    :=                  :explicit=
                    (:< :<= :> :>= :!=) :binary-pred
                    :and                :intersection
                    :in                 :inclusion
                    :including          :including
                    :including-match    :entity-match
                    (:between
                     :<between
                     :between>
                     :<between>)        :range))))

(defn- attr-ref
  "Given an attribute keyword, return a symbol that will represent
  the value in the query."
  ([opts]
   #(attr-ref % opts))
  ([k {:keys [entity-ref suffix] :or {entity-ref '?x}}]
   (if (= :id k)
     entity-ref
     (symbol (str "?"
                (name k)
                suffix)))))

(s/def ::entity-ref symbol?)
(s/def ::remap (s/map-of keyword? keyword?))
(s/def ::target keyword?)
(s/def ::relationship (s/or :plain   (s/tuple keyword? keyword?)
                            :aliased (s/tuple keyword? keyword? keyword?)))
(s/def ::relationships (s/coll-of ::relationship :kind set?))
(s/def ::graph-apex keyword?)
(s/def ::options (s/keys :opt-un [::entity-ref
                                  ::remap
                                  ::target
                                  ::relationships
                                  ::graph-apex]))

(defn- edge->join-clause
  "Given a graph edge (connection between two namespaces), a source
  namespace, and a set of relationships, return a where clause that
  joins the two namespaces."
  [edge source relationships]
  (let [rels (->> relationships
                  (map (fn [[k v]]
                         [#{k v}
                          [k v]]))
                  (into {}))
        rel-key (set edge)
        aliases (->> relationships
                     (filter #(= 3 (count %)))
                     (map (fn [[k v a]]
                            [#{k v} a]))
                     (into {}))
        [parent child] (rels rel-key)
        entity-ref (if (= source child)
                     '?x
                     (symbol (format "?%s" (name child))))
        attr (keyword (name child)
                      (name (aliases rel-key parent)))
        other-entity-ref (if (= source parent)
                           '?x
                           (symbol (format "?%s" (name parent))))]
    [entity-ref
     attr
     other-entity-ref]))

(defn- path->join-clauses
  "Given a path that maps connections between namespaces, return a list
  of where clauses that join the namespaces."
  [path source relationships]
  (->> path
       (partition 2 1)
       (map #(edge->join-clause % source relationships))))

(defn- extract-joining-clauses
  [namespaces {:as ctx :keys [target graph relationships]}]
  (let [source (keyword target)]
    (when (< 1 (count (cond-> namespaces
                        source (conj source))))

      (when-not source
        (throw (ex-info "No target specified and unable to infer one." ctx)))

      (let [targets (difference namespaces #{source})
            paths (g/shortest-paths source
                                    targets
                                    :graph graph)]
        (when-not (seq paths)
          (throw (ex-info (format "No path found from %s to %s"
                                  source
                                  targets)
                          {:source source
                           :targets targets
                           :relationships relationships})))

        (mapcat #(path->join-clauses % source relationships)
                paths)))))

(defn- extract-joining-clauses-from-criteria
  "Given a criteria (map or vector) return the where clauses
  that join the different namespaces."
  [x ctx]
  (let [namespaces (difference
                     (c/namespaces x {:as-keywords true})
                     #{:stowaway.datalog})]
    (extract-joining-clauses namespaces ctx)))

(defn- extract-joining-clauses-from-attributes
  [attrs opts]
  (extract-joining-clauses (->> attrs
                                (map (comp keyword namespace))
                                (filter identity)
                                set)
                           opts))

(defn- attr->sortable
  [shortest-path entities]
  (juxt (comp count
              nodes-in-path
              shortest-path
              keyword
              namespace)
        (comp #(if (entities %)
                 0 1)
              keyword
              name)))

(defn- clause->sortable
  "Given a where clause like [?e :user/first-name ?first-name],
  extract the attribute at index 1 and return a tuple containing
  the distance from the apex node in the 1st position and either a 1
  (if the attribute is an entity reference) or a 0 in the second."
  [clause shortest-path entities]
  (if (or (list? clause) ; TODO: Are both of these really possible?
          (list? (first clause)))
    [999 0] ; a list is used for arbitrary predicates like comparisons, etc.
    (let [f (attr->sortable shortest-path entities)]
      (-> clause
          second
          f))))

(defn- compare-where-clauses
  [shortest-path entities]
  (fn [& clauses]
    (apply compare
           (map #(clause->sortable % shortest-path entities)
                clauses))))

(defn- sort-where-clauses
  "Given a sequence of where clause, sort the clauses with the aim of putting
  the most restrictive clauses first. To achieve this, put entities higher in
  the relationship hierarchy first."
  [{:keys [relationships graph-apex graph] :as ctx}]
  (let [entities (->> relationships seq flatten set)
        shortest #(shortest-path graph graph-apex %)]
    (update-in ctx
               [:query :where]
               #(sort (compare-where-clauses shortest entities)
                      %))))

(defmulti ^:private criterion->inputs
  (fn [[_ v]]
    (when (vector? v)
      (let [[oper] v]
        (case oper
          (:> :>= :< :<= :!= := :including :between :<between :between> :<between>)
          :pred

          :in
          :inclusion

          :including-match
          :match

          (:and :or)
          :conjunction

          nil)))))

(defmethod criterion->inputs :default
  [criterion]
  [criterion])

(defmethod criterion->inputs :pred
  [[k [_ & vs]]]
  (mapv (fn [v]
         [k v])
       vs))

(defmethod criterion->inputs :inclusion
  [[k [_ vs]]]
  [[k (set vs)]])

(defmethod criterion->inputs :match
  [[_k [_ m]]]
  (seq m))

(defmethod criterion->inputs :conjunction
  [[k [_oper & criterions]]]
  (map (fn [[_ v]]
         [k v])
       criterions))

(defmulti ^:private extract-inputs* type-dispatch)

; e.g. {:first-name "John"}
(defmethod extract-inputs* ::stow/map
  [m {:keys [next-ident existing entity-ref]}]
  (->> m
       (mapcat criterion->inputs)
       (filter second)
       (reduce (fn [res [k v]]
                 (update-in res [k v] #(if %
                                         %
                                         (if (= ::id k)
                                           entity-ref
                                           (next-ident)))))
               existing)))

; e.g. [:or {:first-name "John"} {:last-name "Doe"}]
(defmethod extract-inputs* ::stow/vector
  [[_ & cs] {:keys [existing] :as opts}]
  (reduce #(extract-inputs* %2 (assoc opts :existing %1))
          existing
          cs))

(defn- dispense [vals]
  (let [index (atom -1) ]
    (fn []
      (nth vals (swap! index inc)))))

(defn- extract-inputs
  [{:keys [criteria] :as ctx}]
  (extract-inputs* criteria
                   (assoc ctx
                          :next-ident (dispense (map (comp symbol
                                                           #(str "?" %)
                                                           name)
                                                     [:a :b :c :d :e :f :g :h :i]))
                          :existing {})))

(defn- input-map->lists
  [m]
  (->> m
       (mapcat (fn [[k v]]
                 (map #(apply vector k %)
                      v)))
       (reduce (fn [res [_ input-val input-ref]]
                 (-> res
                     (update-in [0] conj input-ref)
                     (update-in [1] conj input-val)))
               [[] []])))

(defn- criterion-e
  [k {:keys [entity-ref target]}]
  (if target
    (if (or (= :id k)
            (= (name target)
               (namespace k)))
      entity-ref
      (symbol (str "?" (namespace k))))
    entity-ref))

(defmulti ^:private criterion->where dispatch-criterion)

(defmethod criterion->where :default
  [[k v :as criterion] {:keys [inputs-map remap] :as opts}]
  ; we're handling :id with special logic, as it can be specified in the
  ; :in clause as the entity reference directly for simple equality tests
  ; We don't want to do that, though, if the attribute has been remapped
  ; the remap is written by the caller and doesn't know about our special
  ; handling for :id
  (let [orig-k (if (= ::id k) :id k)
        attr (get-in remap [orig-k] orig-k)]
    (when (not= :id attr)
      [(if v
         [(criterion-e k opts)
          attr
          (get-in inputs-map criterion)]
         [(list 'missing? '$ (criterion-e k opts) attr)])])))

(defmethod criterion->where :explicit=
  [[k [_ v]] {:keys [inputs-map remap] :as opts}]
  [[(criterion-e k opts) (get-in remap [k] k) (get-in inputs-map [k v])]])

(defmethod criterion->where :inclusion
  [[k [_ vs]] {:as opts :keys [inputs-map]}]
  (let [e-ref (criterion-e k opts)
        a-ref (attr-ref k opts)
        input (get-in inputs-map [k (set vs)])]
    (cond->> [[(list 'contains? input a-ref)]]
      (not= :id k) (cons [e-ref k a-ref]))))

(defmethod criterion->where :binary-pred
  [[k [pred v]] {:keys [inputs-map remap] :as opts}]
  (let [in (get-in inputs-map [k v])
        attr (get-in remap [k] k)
        e-ref (criterion-e k opts)]
    (if (= :id attr)
      [[(list (-> pred name symbol) e-ref in)]]
      (let [ref (symbol (str "?" (name k)))]
        [[e-ref attr ref]
         [(list (-> pred name symbol) ref in)]]))))

(defmethod criterion->where :range
  [[k [pred v1 v2]] {:keys [inputs-map remap] :as opts}]
  (let [in1 (get-in inputs-map [k v1])
        in2 (get-in inputs-map [k v2])
        attr (get-in remap [k] k)
        e-ref (criterion-e k opts)
        ref (symbol (str "?" (name k)))
        [pred1 pred2] (case pred
                        :between '[>= <=]
                        :<between '[> <=]
                        :between> '[>= <]
                        :<between> '[> <])]
    [[e-ref attr ref]
     [(list pred1 ref in1)]
     [(list pred2 ref in2)]]))

(defmethod criterion->where :intersection
  [[k [_ & cs]] {:keys [inputs-map remap] :as opts}]
  (let [ref (symbol (str "?" (name k)))]
    (apply vector
           [(criterion-e k opts) (get-in remap [k] k) ref]
           (map (fn [[pred v]]
                  [(list (-> pred name symbol) ref (get-in inputs-map [k v]))])
                cs))))

(defmethod criterion->where :entity-match
  [[k [_ match]] {:keys [entity-ref remap] :as opts}]
  (let [other-ent-ref (symbol (str "?" (singular (name k))))]
      (cons [entity-ref (get-in remap [k] k) other-ent-ref]
            (mapcat #(criterion->where % (assoc opts :entity-ref other-ent-ref))
                    match))))

(defmethod criterion->where :including
  [[k [_ match]] {:keys [entity-ref inputs-map]}]
  [[entity-ref k (get-in inputs-map [k match])]])

(defmulti ^:private criteria->where type-dispatch)

(defmethod criteria->where ::stow/map
  [criteria opts]
  (vec (mapcat #(criterion->where % opts) criteria)))

(defmethod criteria->where ::stow/vector
  [[conj & cs :as criteria] opts]
  (if-let [simplified (c/simplify-and criteria)]
    (criteria->where simplified opts)
    (let [where (mapcat #(criteria->where % opts) cs)]
      (case conj
        :and
        (if (every? vector? where)
          (vec where)
          [(apply list
                  (-> conj name symbol)
                  where)])

        :or
        [(apply list 'or-join (mapv last where) where)]))))

(defn- parse-id-in-criterion-value
  [v]
  (if (vector? v)
    (update-in v [1] parse-id)
    (parse-id v)))

(defn- normalize-criterion
  [x {:keys [remap]}]
  (if (map-entry? x)
    (let [[k v] x]
      (cond
        (c/model-ref? v)
        (update-in x [1] (comp parse-id :id))

        (= :id k)
        (cond-> (update-in x [1] parse-id-in-criterion-value)
          (not (or (vector? v)
                   (remap k)))
          (assoc 0 ::id)) ; this allows us to peform special handling for entity-ref later in the process

        :else
        x))
    x))

(defn- normalize-criteria
  [ctx]
  (update-in ctx
             [:criteria]
             #(prewalk (fn [c] (normalize-criterion c ctx))
                       %)))

(def ^:private default-apply-criteria-options
  {:entity-ref '?x
   :remap {}})

(defn- strip-redundant-and
  [where]
  (if (and (= 1 (count where))
           (list? (first where))
           (= 'and (ffirst where)))
    (apply vector (rest (first where)))
    where))

(defn- infer-target-from-where
  [where {:keys [entity-ref] :or {entity-ref '?x}}]
  (->> where
       (filter #(= entity-ref (first %)))
       (map #(-> % second namespace))
       first))

(defn- infer-target
  [{:keys [criteria target query] :as ctx}]
  (assoc ctx :target (or target
                         (some-> criteria c/single-ns keyword)
                         (infer-target-from-where (:where query) ctx))))

(defn- calculate-graph
  [{:keys [relationships] :as ctx}]
  (cond-> ctx
    relationships (assoc :graph (apply uber/graph
                                       (map (comp vec (partial take 2))
                                            relationships)))))

(defn- map-inputs
  [ctx]
  (let [inputs-map (extract-inputs ctx)
        [inputs args] (input-map->lists inputs-map)]
    (-> ctx
        (assoc :inputs-map inputs-map
               :inputs inputs
               :args args))))

(defn- apply-criteria-to-where
  [{:keys [criteria query] :as ctx}]
  (assoc-in ctx
            [:query :where]
            (->> (criteria->where criteria ctx)
                 strip-redundant-and
                 (concat (:where query)
                         (extract-joining-clauses-from-criteria criteria ctx))
                 vec)))

(defn- apply-criteria-to-in
  [{:as ctx :keys [inputs]}]
  (update-in ctx
             [:query :in]
             (fnil concat [])
             inputs))

(defn- recursive-id-match
  [{[rel-key upward?] :recursion}]
  [[(list 'match-and-recurse '?x '?target)
    ['(= ?x ?target)]]
   [(list 'match-and-recurse '?x1 '?target)
    (cond-> ['?x1 rel-key '?x2]
      upward? reverse)
    (list 'match-and-recurse '?x2 '?target)]])

(defn- recursive-attr-match
  [{:keys [inputs]
    [rel-key upward?] :recursion
    {:keys [where]} :query}]
  [(apply vector
          (apply list 'match-and-recurse '?x inputs)
          where)
   [(apply list 'match-and-recurse '?x1 inputs)
    (cond-> ['?x1 rel-key '?x2]
      upward? reverse)
    (apply list 'match-and-recurse '?x2 inputs)]])

(defn- apply-recursive-id-match
  [{:as ctx :keys [entity-ref]}]
  (-> ctx
      (assoc :inputs '[% ?id] )
      (update-in [:args] #(cons (recursive-id-match ctx) %))
      (update-in [:query :where] #(concat % [(list 'match-and-recurse entity-ref '?id)]))))

(defn- apply-recursive-attr-match
  [{:as ctx :keys [entity-ref inputs]}]
  (-> ctx
      (update-in [:inputs] (fn [inputs] (cons '% inputs)))
      (update-in [:args] #(cons (recursive-attr-match ctx) %))
      (assoc-in [:query :where] [(apply list 'match-and-recurse entity-ref inputs)])))

(defn- apply-recursion
  [{:as ctx :keys [recursion criteria]}]
  (if recursion
    (if (= #{::id} (->> criteria keys set))
      (apply-recursive-id-match ctx)
      (apply-recursive-attr-match ctx))
    ctx))

(defn- apply-criteria-to-args
  [{:as ctx :keys [args]}]
  (update-in ctx [:query :args] (fnil concat []) args))

(defn apply-criteria
  [query criteria & [options]]
  {:pre [(s/valid? ::c/criteria criteria)
         (s/valid? (s/nilable ::options) options)]}
  (-> default-apply-criteria-options
      (merge options
             {:query query
              :criteria criteria})
      infer-target
      calculate-graph
      normalize-criteria
      map-inputs
      apply-criteria-to-where
      sort-where-clauses
      apply-recursion
      apply-criteria-to-in
      apply-criteria-to-args
      :query))

(defn- ->vector
  [x]
  ((if (sequential? x) vec vector) x))

(defn- apply-select-to-find
  [{:keys [select replace] :or {replace false} :as ctx}]
  (let [attrs (map (attr-ref ctx) select)]
    (if replace
      (assoc-in ctx [:query :find] attrs)
      (update-in ctx [:query :find] concat attrs))))

(defn- apply-select-to-where
  [{:keys [select
           entity-ref
           target]
    :or {entity-ref '?x}
    :as ctx}]
  (update-in ctx
             [:query :where]
             concat
             (->> select
                  (remove #(= :id %))
                  (map #(let [n (namespace %)]
                          (vector (if (= target n)
                                    entity-ref
                                    (symbol (str "?" n)))
                                  %
                                  (attr-ref % ctx)))))
             (extract-joining-clauses-from-attributes select ctx)))

(s/def ::select (s/or :scalar keyword?
                      :vector (s/coll-of keyword?)))

(s/def ::replace boolean?)

(s/def ::select-opts (s/nilable (s/keys :opt-un [::replace
                                                 ::entity-ref])))

(defn apply-select
  [query select & [opts]]
  {:pre [(s/valid? ::select select)
         (s/valid? ::select-opts opts)]}
  (-> opts
      (merge {:query query
              :select (->vector select)})
      infer-target
      calculate-graph
      apply-select-to-find
      apply-select-to-where
      :query))

(defn- ensure-attr
  [{:keys [where] :as query} k arg-ident]
  (if (some #(= arg-ident (last %))
            where)
    query
    (update-in query [:where] conj* ['?x k arg-ident])))

(defmulti apply-sort-segment*
  (fn [_query seg _opts]
    (type seg)))

(defmethod apply-sort-segment* :default
  [query seg opts]
  (apply-sort-segment* query [seg :asc] opts))

(defmethod apply-sort-segment* ::stow/vector
  [query [k dir] opts]
  (let [arg-ident (attr-ref k opts)]
    (-> query
        (ensure-attr k arg-ident)
        (update-in [:find] conj* arg-ident)
        (update-in [:order-by] conj* [arg-ident dir]))))

(defn- apply-sort-segment
  [opts]
  (fn [query seg]
    (apply-sort-segment* query seg opts)))

(defn- apply-sort
  [query order-by & [opts]]
  (reduce (apply-sort-segment opts)
          query
          (->vector order-by)))

(defn apply-options
  [query {:keys [limit offset order-by]} & {:as opts}]
  (cond-> query
    limit (assoc :limit limit)
    offset (assoc :offset offset)
    order-by (apply-sort order-by opts)))
