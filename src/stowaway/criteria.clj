(ns stowaway.criteria
  (:refer-clojure :exclude [update-in])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.set :refer [union
                                 intersection]]
            [stowaway.util :refer [type-dispatch]]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.PersistentHashMap ::map)

(defmulti ^:private criteria-value type)

(s/def ::conjunction #{:or :and})

(s/def ::predicate #{:or
                     :and
                     :in
                     :any
                     :&&
                     :=
                     :>
                     :>=
                     :<
                     :<=
                     :!=
                     :like
                     :between
                     :between>
                     :<between
                     :<between>
                     :contained-by
                     :including
                     :including-match}) ; including-match maps to $elemMatch in mongo

(defmethod criteria-value :default
  [_]
  (s/spec any?))

(defmethod criteria-value nil
  [_]
  (s/spec nil?))

(defmethod criteria-value ::map
  [_]
  (s/and map? #(contains? % :id)))

(defmethod criteria-value ::vector
  [_]
  (s/cat
    :predicate ::predicate
    :rest      (s/+ any?)))

(s/def ::criteria-value (s/multi-spec criteria-value type))

(defmulti ^:private criteria type)

(defmethod criteria ::map
  [_]
  (s/map-of keyword? ::criteria-value))

(defmethod criteria ::vector
  [_]
  (s/cat
    :conjunction ::conjunction
    :rest (s/+ ::criteria)))

(s/def ::criteria (s/multi-spec criteria type))

(defmulti namespaces type-dispatch)

(defmethod namespaces ::map
  [m & [{:keys [as-keywords]}]]
  (let [xform (if as-keywords keyword identity)]
    (->> (keys m)
         (map (comp xform
                    namespace))
         (filter identity)
         (into #{}))))

(defmethod namespaces ::vector
  [[_oper & criterias] & [opts]]
  (->> criterias
       (map #(namespaces % opts))
       (reduce union)))

(defmulti extract-ns
  "Given a criteria and a namespace, extract the portions of the criteria
  that are applicable to the namespace."
  (fn [x _] (type x)))

(defmethod extract-ns ::map
  [criteria n]
  (when-let [entries (->> (update-keys criteria
                                       (comp #(map keyword %)
                                             (juxt namespace name)))
                          (filter #(= n (ffirst %)))
                          (map #(clojure.core/update-in % [0] (comp keyword second)))
                          seq)]
    (into {} entries)))

(defmethod extract-ns ::vector
  [[oper & criterias] n]
  (when-let [extracted (->> criterias
                            (map #(extract-ns % n))
                            (filter identity)
                            seq)]
    (if (= 1 (count extracted))
      (first extracted)
      (apply vector oper extracted))))

(defn single-ns
  "Give a criteria (map or vector), returns the single namepace used in all of the keys,
  or returns nil if multiple namespaces are used."
  [criteria]
  (let [ns (namespaces criteria)]
    (when (= 1 (count ns))
      (first ns))))

(defn simplify-and
  [[oper & cs]]
  (and (= :and oper)
       (every? map? cs)
       (reduce (fn [c1 c2]
                 (let [dup-keys (intersection (set (keys c1))
                                              (set (keys c2)))]
                   (if (or (empty? dup-keys)
                           (every? #(= (c1 %) (c2 %)) dup-keys))
                     (merge c1 c2)
                     (reduced false))))
               cs)))

(defn- one?
  [c]
  (= 1 (count c)))

(def model-ref?
  (every-pred map?
              one?
              #(= :id (first (keys %)))))

(defmulti ^:private apply-to-value*
  "Apply a function with optional arguments to a criterion value. If the value
  contains an explicit operator, like {:user/age [:>= 21]}, the function is
  applied to the values after the operator"
  (fn [v & _]
    (type v)))

(defmethod apply-to-value* :default
  [v f args]
  (apply f v args))

(defmethod apply-to-value* ::vector
  [[oper & vs] f args]
  (apply vector oper (map #(apply f % args)
                          vs)))

(defn- apply-to-value
  [f args]
  (fn [v]
    (apply-to-value* v f args)))

(defn- deep-contains?
  [m ks]
  (if (= 1 (count ks))
    (contains? m (first ks))
    (deep-contains? (get-in m (take 1 ks))
                    (rest ks))))

(defn update-in
  "Given a criteria map, a key sequence and a function, update the value at the
  specified key, accounting for a value that includes an explicit operation,
  like {:user/age [:>= 21]}. Additional, if the specified criteria map does not
  contain the specified key, no action is performed."
  [c ks f & args]
  (if (deep-contains? c ks)
    (clojure.core/update-in c ks (apply-to-value f args))
    c))

(defmulti apply-to
  "Given a criteria (map or vector) apply the given function to each map"
  (fn [c _] (type c)))

 (defmethod apply-to ::map
   [criteria f & args]
   (apply f criteria args))

(defmethod apply-to ::vector
  [[oper & cs :as criteria] f & args]
  (with-meta (apply vector
                    oper
                    (map #(apply apply-to % f args) cs))
             (meta criteria)))
