(ns stowaway.util
  (:require [clojure.walk :refer [prewalk]]))

(defn- deep-contains?
  [m [k & ks]]
  (if (empty? ks) 
    (contains? m k)
    (deep-contains? (m k) ks)))

(defn update-in-if
  [m k f]
  (if (deep-contains? m k)
    (update-in m k f)
    m))

(defn- key-value-tuple?
  [x]
  (and (vector? x)
       (= 2 (count x))
       (keyword? (first x))))

(defmulti qualify-key
  (fn [x & _]
    (when (key-value-tuple? x)
      :tuple)))

(defmethod qualify-key :default
  [x & _]
  x)

(defmethod qualify-key :tuple
  [[k :as x] nspace {:keys [ignore?]}]
  (if (ignore? k)
    x
    (update-in x [0] #(keyword nspace (name %)))))

(defn qualify-keys
  "Creates fully-qualified entity attributes by applying
  the :model-type from the meta data to the keys of the map."
  [m ns-key & {:keys [ignore]}]
  {:pre [(map? m)]}
  (let [k (if (keyword? ns-key)
            (name ns-key)
            ns-key)
        ignore? (if ignore
                  (some-fn ignore namespace)
                  namespace)]
    (prewalk #(qualify-key % k {:ignore? ignore?})
             m)))

(defn unqualify-keys
  "Replaces qualified keys with the simple values"
  [m]
  (prewalk (fn [x]
             (if (key-value-tuple? x)
               (update-in x [0] (comp keyword name))
               x))
           m))
