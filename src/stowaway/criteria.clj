(ns stowaway.criteria
  (:require [clojure.pprint :refer [pprint]]
            [clojure.set :refer [union]]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.PersistentHashMap ::map)

(defmulti namespaces (fn [c & _] (type c)))

(defmethod namespaces ::map
  [m & {:keys [as-keywords]}]
  (let [xform (if as-keywords keyword identity)]
    (->> (keys m)
         (map (comp xform
                    namespace))
         (filter identity)
         (into #{}))))

(defmethod namespaces ::vector
  [[_oper & criterias]]
  (->> criterias
       (map namespaces)
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
                          (map #(update-in % [0] (comp keyword second)))
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
