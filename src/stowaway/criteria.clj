(ns stowaway.criteria
  (:require [clojure.pprint :refer [pprint]]
            [clojure.set :refer [union]]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.PersistentHashMap ::map)

(defmulti namespaces type)

(defmethod namespaces ::map
  [m]
  (->> (keys m)
       (map namespace)
       (filter identity)
       (into #{})))

(defmethod namespaces ::vector
  [[_oper & criterias]]
  (->> criterias
       (map namespaces)
       (reduce union)))

(defmulti extract-ns (fn [x _] (type x)))

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
