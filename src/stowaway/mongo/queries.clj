(ns stowaway.mongo.queries
  (:require [clojure.pprint :refer [pprint]]
            [clojure.walk :refer [postwalk]]
            [clojure.string :as str]
            [clojure.set :refer [rename-keys]]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.util :refer [unqualify-keys
                                   update-in-if]]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentHashMap ::map)
(derive clojure.lang.PersistentArrayMap ::map)

(defn- contains-mongo-keys?
  [m]
  (->> (keys m)
       (map name)
       (some #(str/starts-with? % "$"))))

; TODO: I think we need to be smarter about transforming model keys
; verses mongo query keys, but this will keep us moving for now
(defn- ->mongo-keys
  [m]
  (postwalk (fn [x]
              (if (and (map? x)
                       (not (contains-mongo-keys? x)))
                (update-keys x ->snake_case)
                x))
            m))

(def ^:private oper-map
  {:> :$gt
   :>= :$gte
   :< :$lt
   :<= :$lte
   :!= :$ne})

(defmulti ^:private adjust-complex-criterion
  (fn [[_k v]]
    (when (vector? v)
      (let [[oper] v]
        (or (#{:and :or} oper)
            (when (oper-map oper) :comparison)
            (first v))))))

(defn- ->mongodb-op
  [op]
  (get-in oper-map
          [op]
          (keyword (str "$" (name op)))))

(defmulti ^:private ->mongodb-sort type)

(defmethod ->mongodb-sort :default
  [x]
  [(->snake_case x) 1])

(defmethod ->mongodb-sort ::vector
  [sort]
  (-> sort
      (update-in [0] ->snake_case)
      (update-in [1] #(if (= :asc %) 1 -1))))

(defmethod adjust-complex-criterion :default [c] c)

(defmethod adjust-complex-criterion :comparison
  [[f [op v]]]
  ; e.g. [:transaction-date [:< #inst "2020-01-01"]]
  ; ->   [:transaction-date {:$lt #inst "2020-01-01"}]
  {f {(->mongodb-op op) v}})

(defmethod adjust-complex-criterion :and
  [[f [_ & cs]]]
  {f (->> cs
          (map #(update-in % [0] ->mongodb-op))
          (into {}))})

#_(defmethod adjust-complex-criterion :or
  [[f [_ & cs]]]
  {f {:$or (mapv (fn [[op v]]
                   {(->mongodb-op op) v})
                 cs)}})

(defn- adjust-complex-criteria
  [criteria]
  (->> criteria
       (map adjust-complex-criterion)
       (into {})))

(defmulti ^:private translate-criteria (fn [criteria _] (type criteria)))

(defmethod translate-criteria ::map
  [criteria {:keys [coerce-id] :or {coerce-id identity}}]
  (-> criteria
        unqualify-keys
        ->mongo-keys
        (update-in-if [:id] coerce-id)
        (rename-keys {:id :_id})
        adjust-complex-criteria))

(defmethod translate-criteria ::vector
  [[oper & crits] opts]
  (if (= :or oper)
    {:$or (mapv #(translate-criteria % opts) crits)}
    (apply merge crits)))

(defn- apply-options
  [query {:keys [limit order-by sort]}]
  (let [srt (or sort order-by)]
    (cond-> query
      limit (assoc :limit limit)
      srt (assoc :sort (map ->mongodb-sort srt)))))

(defn criteria->query
  ([criteria] (criteria->query criteria {}))
  ([criteria options]
   (let [where (translate-criteria criteria options)]
     (cond-> (apply-options {} options)
       (seq where) (assoc :where where)))))
