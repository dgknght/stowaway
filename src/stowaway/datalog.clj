(ns stowaway.datalog
  (:require [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]]
            [clojure.set :refer [difference]]
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
  (fn [_query [_k v]]
    (when (vector? v)
      (case (first v)
        :=                  :direct
        (:< :<= :> :>= :!=) :binary-pred
        :and                :intersection
        :or                 :union))))

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

(defn- param-ref
  "Given an attribute keyword, return a symbol that will represent
  an input value in the query"
  [k]
  (if (id? k)
    '?x
    (attr-ref k "-in")))

(defn- ref-var
  [k]
  (let [target (:target *opts*)
        n (namespace k)]
    (if (or (nil? target)
            (= target
               (keyword n)))
      '?x
      (symbol (str "?" n)))))

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
  ([w k input pred]
   (let [attr (attr-ref k)
         assignment-ref (if pred
                          attr
                          input)
         ref-var (ref-var k)
         assignment (when-not (id? k)
                      [ref-var
                       (remap k)
                       assignment-ref])
         predicate (when pred
                     [(list (symbol (name pred))
                            attr
                            input)])]
     (concat* w
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

(defn apply-criteria
  "Given a datalog query and a criteria (map or vector), return
   the query with additional attributes that match the specified criteria."
  [query criteria & {:as opts}]
  {:pre [(or (nil? opts)
             (s/valid? ::options opts))]}

  (with-options opts
    (append-joining-clauses
      (reduce apply-criterion
              query
              criteria)
      criteria)))

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
