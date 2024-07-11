(ns stowaway.datalog
  (:require [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]]
            [clojure.set :refer [difference]]
            [ubergraph.core :as uber]
            [ubergraph.alg :refer [shortest-path
                                   nodes-in-path]]
            [stowaway.core :as stow]
            [stowaway.util :refer [type-dispatch]]
            [stowaway.inflection :refer [singular]]
            [stowaway.criteria :as c]
            [stowaway.graph :as g]))

(def ^:private concat*
  (fnil (comp vec concat) []))

(def ^:private conj*
  (fnil conj []))

(def ^{:private true :dynamic true} *opts*
  {:coerce identity
   :args-key [:args]
   :query-prefix []
   :remap {}})

(defn- coerce
  [x]
  ((:coerce *opts*) x))

(defn- args-key []
  (:args-key *opts*))

(defn- remap
  [k]
  (get-in (:remap *opts*) [k] k))

(defn- query-key
  [& ks]
  (concat (:query-prefix *opts*) ks))

(defmulti apply-criterion
  (fn [_query c]
    {:pre [(vector? c)]}
    (let [[_k v] c]
      (cond
        (map? v)    :model-ref
        (vector? v) (case (first v)
                      :=                  :direct
                      (:< :<= :> :>= :!=) :binary-pred
                      :and                :intersection
                      :or                 :union
                      :including          :entity-match)))))

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

(defn- param-ref
  "Given an attribute keyword, return a symbol that will represent
  an input value in the query"
  ([k] (param-ref k *opts*))
  ([k opts]
   (if (id? k)
     (ref-var k opts)
     (attr-ref k "-in"))))

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
  [query k v]
  (let [input (param-ref k)]
    (-> query
        (update-in (query-key :where) append-where k input)
        (update-in (query-key :in) conj* input)
        (update-in (args-key) conj* (coerce v)))))

(defmethod apply-criterion :default
  [query [k v]]
  (apply-simple-criterion query k v))

(defmethod apply-criterion :model-ref
  [query c]
  (apply-criterion query (update-in c [1] :id)))

(defmethod apply-criterion :direct
  [query [k [_oper v]]]
  (apply-simple-criterion query k v))

(defmethod apply-criterion :binary-pred
  [query [k [pred v]]]
  (let [input (attr-ref k "-in")]
    (-> query
        (update-in (args-key) conj* (coerce v))
        (update-in (query-key :in) conj* input)
        (update-in (query-key :where) append-where k input pred))))

(defmethod apply-criterion :intersection
  [query [k [_and & vs]]]
  ; 1. establish a reference to the model attribute
  ; 2. apply each comparison to the reference
  ; 3. decide if we need to wrap with (and ...)
  (let [attr (name k)
        input-refs (map (comp symbol
                              #(str "?" attr "-in-" %)
                              #(+ 1 %))
                        (range (count vs)))
        attr-ref (attr-ref k)]
    (-> query
        (update-in (args-key) concat* (map (comp coerce last) vs))
        (update-in (query-key :in)      concat* input-refs)
        (update-in (query-key :where)
                   concat*
                   (cons
                     ['?x (remap k) attr-ref]
                     (->> vs
                        (interleave input-refs)
                        (partition 2)
                        (map (fn [[input-ref [oper]]]
                               [(list (-> oper name symbol)
                                       attr-ref
                                       input-ref)]))))))))

(defn- apply-map-match
  [query k match]
  (let [other-ent-ref (symbol (str "?" (singular (name k))))]
    (reduce (fn [q [k v]]
              (let [val-in (symbol (str "?" (name k) "-in"))]
                (-> q
                    (update-in (query-key :where) conj [other-ent-ref k val-in])
                    (update-in (query-key :in) conj* val-in)
                    (update-in (args-key) conj* v))))
            (update-in query (query-key :where) conj* ['?x (remap k) other-ent-ref])
            match)))

(defn- apply-tuple-match
  [query k match]
  (apply-simple-criterion query k match))

(defmethod apply-criterion :entity-match
  [query [k [_ match]]]
  (cond
    (map? match)    (apply-map-match query k match)
    (vector? match) (apply-tuple-match query k match)))

(s/def ::args-key (s/coll-of keyword? :kind vector?))
(s/def ::query-prefix (s/coll-of keyword :kind vector?))
(s/def ::options (s/keys :opt-un [::args-key
                                  ::query-prefix]))

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
  [criteria]
  (let [namespaces (c/namespaces criteria :as-keywords true)]
    (when (< 1 (count namespaces))
      (let [source (or (:target *opts*)
                       (throw (ex-info "No target specified for criteria."
                                       {:options *opts*
                                        :criteria criteria})))
            targets (difference namespaces #{source})
            rels (:relationships *opts*)
            paths (g/shortest-paths source
                                    targets
                                    rels)]
        (mapcat #(path->join-clauses % source rels)
                paths)))))

(defn- append-joining-clauses
  "Given a datalog query and a criteria (map or vector), when the criteria
  spans multiple namespaces, return the query with addition where clauses necessary
  to join the namespaces."
  [query criteria]
  (update-in query
             (query-key :where)
             concat
             (extract-joining-clauses criteria)))

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
  (if (list? (first clause))
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
  [query {:keys [relationships graph-apex]}]
  (let [graph (apply uber/graph relationships) ; TODO: Rework this so we're not creating the graph twice
        entities (->> relationships seq flatten set)
        shortest #(shortest-path graph graph-apex %)]
    (update-in query
               (query-key :where)
               #(sort (compare-where-clauses shortest entities)
                      %))))

(defmulti ^:private extract-inputs* type-dispatch)

(defmethod extract-inputs* ::stow/map
  [m next-ident]
  (reduce (fn [res k]
            (assoc-in res k (symbol (str "?" (next-ident)))))
          {}
          m))

(defmethod extract-inputs* ::stow/vector
  [[_ & cs] next-ident]
  ; TODO: figure out how to merge these
  (extract-inputs* (first cs) next-ident))

(defn- dispense [vals]
  (let [index (atom -1) ]
    (fn []
      (nth vals (swap! index inc)))))

(defn- extract-inputs
  [criteria]
  (extract-inputs* criteria (dispense [:a :b :c :d :e :f :g :h :i])))

(defmulti ^:private apply-criteria*
  "Given a datalog query and a criteria (map or vector), return
  the query with additional attributes that match the specified criteria."
  (fn [{:keys [criteria]}]
    (type criteria)))

(defmethod apply-criteria* ::stow/map
  [{:keys [criteria options] :as context}]
  (with-options options
    (update-in context
               [:query] #(-> (reduce apply-criterion
                                     %
                                     criteria)
                             (append-joining-clauses criteria)
                             (sort-where-clauses options)))))

(defmethod apply-criteria* ::stow/vector
  [{:keys [criteria] :as context}]

  (pprint {::apply-criteria* criteria})

  (if-let [simp (c/simplify-and criteria)]
    (apply-criteria* (assoc context :criteria simp))
    (let [[conj & cs] criteria]

      (pprint {:inputs (extract-inputs criteria)})

      (reduce (fn [ctx criteria]
                (apply-criteria*
                  (assoc ctx
                         :criteria criteria
                         :conj conj)))
              context
              cs))))

(defn apply-criteria
  [query criteria & [options]]
  {:pre [(s/valid? ::c/criteria criteria)
         (s/valid? (s/nilable ::options) options)]}

  (:query (apply-criteria* {:query query
                            :criteria criteria
                            :options (or options {})})))

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
