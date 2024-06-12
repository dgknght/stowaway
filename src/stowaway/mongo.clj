(ns stowaway.mongo
  (:require[clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [clojure.set :refer [rename-keys]]
            [clojure.pprint :refer [pprint]]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.core :as s]
            [stowaway.util :refer [unqualify-keys
                                   update-in-if
                                   key-join]]))

(defn- contains-mongo-keys?
  [m]
  (->> (keys m)
       (map name)
       (some #(str/starts-with? % "$"))))

(defn- one?
  [c]
  (= 1 (count c)))

(def ^:private simple-model-ref?
  (every-pred map?
              one?
              #(= :id (first (keys %)))))

(defmulti ^:private ->mongo-key type)

(defmethod ->mongo-key :default [x] x)

(defmethod ->mongo-key ::s/map
  [m]
  (if (contains-mongo-keys? m)
    m
    (update-keys m ->snake_case)))

(defmethod ->mongo-key ::s/map-entry
  [[_ v :as e]]
  (if (simple-model-ref? v)
    (-> e
        (update-in [0] #(key-join % "-id"))
        (update-in [1] :id))
    e))

; TODO: I think we need to be smarter about transforming model keys
; verses mongo query keys, but this will keep us moving for now
(defn ->mongo-keys
  [m]
  (postwalk ->mongo-key m))

(def oper-map
  {:> :$gt
   :>= :$gte
   :< :$lt
   :<= :$lte
   :!= :$ne})

(defn ->mongo-operator
  [op]
  (get-in oper-map
          [op]
          (keyword (str "$" (name op)))))

(defmulti ^:private adjust-complex-criterion
  (fn [[_k v]]
    (when (vector? v)
      (let [[oper] v]
        (or (#{:and :or} oper)
            (when (oper-map oper) :comparison)
            (first v))))))

(defmethod adjust-complex-criterion :default [c] c)

(defmethod adjust-complex-criterion :comparison
  [[f [op v]]]
  ; e.g. [:transaction-date [:< #inst "2020-01-01"]]
  ; ->   [:transaction-date {:$lt #inst "2020-01-01"}]
  {f {(->mongo-operator op) v}})

(defmethod adjust-complex-criterion :and
  [[f [_ & cs]]]
  {f (->> cs
          (map #(update-in % [0] ->mongo-operator))
          (into {}))})

#_(defmethod adjust-complex-criterion :or
  [[f [_ & cs]]]
  {f {:$or (mapv (fn [[op v]]
                   {(->mongo-operator op) v})
                 cs)}})

(defn- adjust-complex-criteria
  [criteria]
  (->> criteria
       (map adjust-complex-criterion)
       (into {})))

(defmulti translate-criteria (fn [criteria _] (type criteria)))

(defmethod translate-criteria ::s/map
  [criteria {:keys [coerce-id] :or {coerce-id identity}}]
  (-> criteria
        unqualify-keys
        ->mongo-keys
        (update-in-if [:id] coerce-id)
        (rename-keys {:id :_id})
        adjust-complex-criteria))

(defmethod translate-criteria ::s/vector
  [[oper & crits] opts]
  (if (= :or oper)
    {:$or (mapv #(translate-criteria % opts) crits)}
    (apply merge crits)))
