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

(def ^:private concat*
  (fnil (comp vec concat) []))

(def ^:private conj*
  (fnil conj []))

(def ^{:private true :dynamic true} *opts*
  {:coerce identity
   :args-key [:args]
   :query-prefix []
   :remap {}})

(defn- remap
  [k]
  (get-in (:remap *opts*) [k] k))

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
                    :or                 :union
                    :including          :including
                    :including-match    :entity-match))))

(defmulti apply-criterion dispatch-criterion)

(defn- id?
  "Returns true if the given keyword specifies an entity id"
  [k]
  (= :id (remap k)))

(defn- attr-ref
  "Given an attribute keyword, return a symbol that will represent
  the value in the query."
  ([k] (attr-ref k nil))
  ([k suffix]
   (symbol (str "?"
                (if (id? k)
                  "x"
                  (name k))
                suffix))))

(defn- ref-var
  "Given a simple keyword representing a criteria namespace (model),
  return a symbol that will be used to reference entities of that
  type in the query.

  E.g.:

  (ref-var :user) => ?user

  ; with *opts* {:vars {:user ?u}}
  (ref-var :user) => ?u"
  ([k] (ref-var k *opts*))
  ([k opts]
   {:pre [k
          (nil? (namespace k))]}

   (let [target (:target opts)
         vars (or (:vars opts) {})
         n (name k)]
     (or (vars k)
         (if (or (nil? target)
                 (= target (keyword n))
                 (= :id k))
           '?x
           (symbol (str "?" n)))))))

(defn- append-where
  "Appends clauses to an existing where clause.

  When given a predicate, appends an assignment clause and a predicate clause.

  [[?x :foo/bar ?bar]
   [(>= ?bar ?bar-in)]]

  When not given a predicate, appends an assignment clause (which is implicitly
  an equality test as well).

  [[?x :foo/bar ?bar-in]]"
  ; NB if we weren't expecting to have (pull ?x [*]) in the :find clause,
  ; then we'd need to bind to the reference without in "-in" suffix.
  ([w k input]
   (append-where w k input nil))
  ([where k input pred]
   (let [attr (attr-ref k)
         assignment-ref (if pred
                          attr
                          input)
         ref-var (ref-var (or (-> k namespace keyword) ; an :id key won't have a namespace. should we allow this?
                              (:target *opts*)
                              k))
         assignment (when-not (id? k)
                      [ref-var
                       (remap k)
                       assignment-ref])
         predicate (when pred
                     [(list (symbol (name pred))
                            attr
                            input)])]
     (concat* where
              (filterv identity
                       [assignment
                        predicate])))))

(defn- apply-simple-criterion
  [query k v inputs]
  (let [input (get-in inputs [k v])]
    (update-in query [:where] append-where k input)))

(defmethod apply-criterion :default
  [query [k v] inputs]
  (apply-simple-criterion query k v inputs))

(defmethod apply-criterion :model-ref
  [query c inputs]
  (apply-criterion query (update-in c [1] :id) inputs))

(defmethod apply-criterion :explicit=
  [query [k [_oper v]] inputs]
  (apply-simple-criterion query k v inputs))

(defmethod apply-criterion :binary-pred
  [query [k [pred v]] inputs]
  (let [input (get-in inputs k v)]
    (update-in query [:where] append-where k input pred)))

(defmethod apply-criterion :intersection
  [query [k [_and & vs]] _inputs]
  ; 1. establish a reference to the model attribute
  ; 2. apply each comparison to the reference
  ; 3. decide if we need to wrap with (and ...)
  (let [attr (name k)
        input-refs (map (comp symbol
                              #(str "?" attr "-in-" %)
                              #(+ 1 %))
                        (range (count vs)))
        attr-ref (attr-ref k)]
    (update-in query
               [:where]
               concat*
               (cons
                 ['?x (remap k) attr-ref]
                 (->> vs
                      (interleave input-refs)
                      (partition 2)
                      (map (fn [[input-ref [oper]]]
                             [(list (-> oper name symbol)
                                    attr-ref
                                    input-ref)])))))))

(defn- apply-map-match
  [query k match]
  (let [other-ent-ref (symbol (str "?" (singular (name k))))]
    (reduce (fn [q [k v]]
              (let [val-in (symbol (str "?" (name k) "-in"))]
                (-> q
                    (update-in [:where] conj [other-ent-ref k val-in])
                    (update-in [:in] conj* val-in)
                    (update-in [:args] conj* v))))
            (update-in query [:where] conj* ['?x (remap k) other-ent-ref])
            match)))

(defn- apply-tuple-match
  [query k match inputs]
  (apply-simple-criterion query k match inputs))

(defmethod apply-criterion :entity-match
  [query [k [_ match]] inputs]
  (cond
    (map? match)    (apply-map-match query k match)
    (vector? match) (apply-tuple-match query k match inputs)))

(s/def ::entity-ref symbol?)
(s/def ::remap (s/map-of keyword? keyword?))
(s/def ::target keyword?)
(s/def ::relationships (s/coll-of (s/tuple keyword? keyword?) :kind set?))
(s/def ::graph-apex keyword?)
(s/def ::options (s/keys :opt-un [::entity-ref
                                  ::remap
                                  ::target
                                  ::relationships
                                  ::graph-apex]))

(defmacro ^:private with-options
  [opts & body]
  `(binding [*opts* (merge *opts* ~opts)]
     ~@body))

(defn- edge->join-clause
  "Given a graph edge (connection between two namespaces), a source
  namespace, and a set of relationships, return a where clause that
  joins the two namespaces."
  [edge source relationships]
  (let [[parent child] (some relationships
                             [edge
                              (reverse edge)])
        entity-ref (if (= source child)
                     '?x
                     (symbol (format "?%s" (name child))))
        attr (keyword (name child)
                      (name parent))
        other-entity-ref (if (= source parent)
                           '?x
                           (symbol (format "?%s" (name parent))))]
    [entity-ref
     attr
     other-entity-ref]))

(defn- path->join-clauses
  "Given a path that maps connections between namespaces, return a list
  of where clauses the join the namespaces."
  [path source relationships]
  (->> path
       (partition 2 1)
       (map #(edge->join-clause % source relationships))))

(defn- extract-joining-clauses
  "Given a criteria (map or vector) return the where clauses
  that join the different namespaces."
  [criteria {:as opts :keys [graph]}]
  (let [namespaces (c/namespaces criteria {:as-keywords true})]
    (when (< 1 (count namespaces))
      (let [source (or (:target opts)
                       (throw (ex-info "No target specified for criteria."
                                       {:options opts
                                        :criteria criteria})))
            targets (difference namespaces #{source})
            rels (:relationships opts)
            paths (g/shortest-paths source
                                    targets
                                    :graph graph)]
        (mapcat #(path->join-clauses % source rels)
                paths)))))

(defn- append-joining-clauses
  "Given a datalog query and a criteria (map or vector), when the criteria
  spans multiple namespaces, return the query with addition where clauses necessary
  to join the namespaces."
  [query criteria opts]
  (if-let [clauses (extract-joining-clauses criteria opts)]
    (update-in query
               [:where]
               concat
               clauses)
    query))

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
  "Given a query with a where clause, sort the clauses with the aim of putting
  the most restrictive clauses first. To achieve this, put entities higher in
  the relationship hierarchy first."
  [query {:keys [relationships graph-apex graph]}]
  (let [entities (->> relationships seq flatten set)
        shortest #(shortest-path graph graph-apex %)]
    (update-in query
               [:where]
               #(sort (compare-where-clauses shortest entities)
                      %))))

(defmulti ^:private criterion->inputs
  (fn [[_ v]]
    (when (vector? v)
      (let [[oper] v]
        (case oper
          (:> :>= :< :<= :in :!= := :including)
          :binary-pred

          :including-match
          :match

          (:and :or)
          :conjunction

          nil)))))

(defmethod criterion->inputs :default
  [criterion]
  [criterion])

(defmethod criterion->inputs :binary-pred
  [[k [_ v]]]
  [[k v]])

(defmethod criterion->inputs :match
  [[_k [_ m]]]
  (seq m))

(defmethod criterion->inputs :conjunction
  [[k [_oper & criterions]]]
  (map (fn [[_ v]]
         [k v])
       criterions))

(defmulti ^:private extract-inputs* type-dispatch)

(defmethod extract-inputs* ::stow/map
  [m {:keys [next-ident existing entity-ref]}]
  (->> m
       (mapcat criterion->inputs)
       (reduce (fn [res [k v]]
                 (update-in res [k v] #(if %
                                         %
                                         (if (= ::id k)
                                           entity-ref
                                           (next-ident)))))
               existing)))

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
  [criteria opts]
  (extract-inputs* criteria
                   (assoc opts
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
  [[k :as criterion] {:keys [inputs remap] :as opts}]
  ; we're handling :id with special logic, as it can be specified in the
  ; :in clause as the entity reference directly for simple equality tests
  ; We don't want to do that, though, if the attribute has been remapped
  ; the remap is written by the caller and doesn't know about our special
  ; handling for :id
  (let [orig-k (if (= ::id k) :id k)
        attr (get-in remap [orig-k] orig-k)]
    (if (= :id attr)
      []
      [[(criterion-e k opts)
        attr
        (get-in inputs criterion)]])))

(defmethod criterion->where :explicit=
  [[k [_ v]] {:keys [inputs remap] :as opts}]
  [[(criterion-e k opts) (get-in remap [k] k) (get-in inputs [k v])]])

(defmethod criterion->where :binary-pred
  [[k [pred v]] {:keys [inputs remap] :as opts}]
  (let [in (get-in inputs [k v])
        attr (get-in remap [k] k)
        e-ref (criterion-e k opts)]
    (if (= :id attr)
      [[(list (-> pred name symbol) e-ref in)]]
      (let [ref (symbol (str "?" (name k)))]
        [[e-ref attr ref]
         [(list (-> pred name symbol) ref in)]]))))

(defmethod criterion->where :intersection
  [[k [_ & cs]] {:keys [inputs remap] :as opts}]
  (let [ref (symbol (str "?" (name k)))]
    (apply vector
           [(criterion-e k opts) (get-in remap [k] k) ref]
           (map (fn [[pred v]]
                  [(list (-> pred name symbol) ref (get-in inputs [k v]))])
                cs))))

(defmethod criterion->where :entity-match
  [[k [_ match]] {:keys [entity-ref remap] :as opts}]
  (let [other-ent-ref (symbol (str "?" (singular (name k))))]
      (cons [entity-ref (get-in remap [k] k) other-ent-ref]
            (mapcat #(criterion->where % (assoc opts :entity-ref other-ent-ref))
                    match))))

(defmethod criterion->where :including
  [[k [_ match]] {:keys [entity-ref inputs]}]
  [[entity-ref k (get-in inputs [k match])]])

(defmulti ^:private criteria->where type-dispatch)

(defmethod criteria->where ::stow/map
  [criteria opts]
  (vec (mapcat #(criterion->where % opts) criteria)))

(defmethod criteria->where ::stow/vector
  [[conj & cs :as criteria] opts]
  (if-let [simplified (c/simplify-and criteria)]
    (criteria->where simplified opts)
    (let [where (mapcat #(criteria->where % opts) cs)]
      (if (and (= :and conj)
               (every? vector? where))
        (vec where)
        [(apply list
                (-> conj name symbol)
                where)]))))

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
  [criteria opts]
  (prewalk #(normalize-criterion % opts)
            criteria))

(def ^:private default-apply-criteria-options
  {:entity-ref '?x
   :remap {}})

(defn apply-criteria
  [query criteria & [options]]
  {:pre [(s/valid? ::c/criteria criteria)
         (s/valid? (s/nilable ::options) options)]}
  (let [opts (merge default-apply-criteria-options
                    {:target (c/single-ns criteria)
                     :graph (when-let [rels (:relationships options)]
                              (apply uber/graph rels))}
                    options)
        normalized (normalize-criteria criteria opts)
        inputs-map (extract-inputs normalized opts)
        [inputs args] (input-map->lists inputs-map)
        where (criteria->where normalized (assoc opts
                                                 :inputs inputs-map))]
    (-> query
        (update-in [:in] (fnil concat []) inputs)
        (update-in [:args]  (fnil concat []) args)
        (update-in [:where] (fn [w]
                              (if w
                                (vec (concat w where))
                                where)))
        (append-joining-clauses normalized opts)
        (sort-where-clauses opts)
        (update-in [:where] vec))))

(defn- ensure-attr
  [{:keys [where] :as query} k arg-ident]
  (if (some #(= arg-ident (last %))
            where)
    query
    (update-in query [:where] conj* ['?x k arg-ident])))

(defmulti apply-sort-segment
  (fn [_query seg]
    (when (vector? seg) :vector)))

(defmethod apply-sort-segment :default
  [query seg]
  (apply-sort-segment query [seg :asc]))

(defmethod apply-sort-segment :vector
  [query [k dir]]
  (let [arg-ident (attr-ref k)]
    (-> query
        (ensure-attr k arg-ident)
        (update-in [:find] conj* arg-ident)
        (update-in [:order-by] conj* [arg-ident dir]))))

(defn- apply-sort
  [query order-by]
  (reduce apply-sort-segment
          query
          (if (coll? order-by)
            order-by
            [order-by])))

(defn apply-options
  [query {:keys [limit offset order-by]} & {:as opts}]
  (with-options opts
    (cond-> query
      limit (assoc :limit limit)
      offset (assoc :offset offset)
      order-by (apply-sort order-by))))
