(ns stowaway.mongo
  (:require[clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [clojure.set :refer [rename-keys]]
            [clojure.pprint :refer [pprint]]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.core :as s]
            [stowaway.criteria :refer [simplify-and]]
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

(defmulti ^:private mongoize (fn [x & _] (type x)))

(defmethod mongoize :default [x & _] x)

(defmethod mongoize ::s/map
  [m _]
  (if (contains-mongo-keys? m)
    m
    (update-keys m ->snake_case)))

(defmethod mongoize ::s/map-entry
  [[_ v :as e] {:keys [coerce-id]}]
  (if (simple-model-ref? v)
    (-> e
        (update-in [0] #(key-join % "_id"))
        (update-in [1] (comp coerce-id :id)))
    e))

; TODO: I think we need to be smarter about transforming model keys
; verses mongo query keys, but this will keep us moving for now
(defn mongoize-criteria
  [m options]
  (postwalk #(mongoize % options) m))

(def oper-map
  {:>         :$gt
   :>=        :$gte
   :<         :$lt
   :<=        :$lte
   :!=        :$ne
   :or        :$or
   :and       :$and
   :including :$elemMatch})

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
            (when (oper-map oper) :operator-swap)
            (first v))))))

(defmethod adjust-complex-criterion :default [c] c)

(defmethod adjust-complex-criterion :operator-swap
  [[f [op v]]]
  ; e.g. [:transaction-date [:< #inst "2020-01-01"]]
  ; ->   [:transaction-date {:$lt #inst "2020-01-01"}]
  {(if (= :including op)
     (-> f name keyword) ; we're expecting this to be plural already
     f)
   {(->mongo-operator op) v}})

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

(def ^:private default-translate-opts
  {:coerce-id identity})

(defmulti translate-criteria (fn [criteria _] (type criteria)))

; kebab-case keys are translated to snake case
; {:user/first-name "John"} => {:first_name "John"}

; simple :id attributes a prefixed with the target collection (singularized)
; {:id 101} => {:user/_id 101}

; model references are convered into forein key names with a simple id value
; {:user {:id 101}} => {:user_id 101}
(defmethod translate-criteria ::s/map
  [criteria options]
  (let [{:keys [coerce-id] :as opts} (merge default-translate-opts options)]
    (-> criteria
        unqualify-keys
        (mongoize-criteria opts)
        (update-in-if [:id] coerce-id)
        (rename-keys {:id :_id})
        adjust-complex-criteria)))

(defmethod translate-criteria ::s/vector
  [[oper & cs :as criteria] opts]
  (if-let [simp (simplify-and criteria)]
    (translate-criteria simp opts)
    {(->mongo-operator oper)
     (mapv #(translate-criteria % opts) cs)}))
