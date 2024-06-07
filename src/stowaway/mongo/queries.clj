(ns stowaway.mongo.queries
  (:require [clojure.pprint :refer [pprint]]
            [camel-snake-kebab.core :refer [->snake_case]]
            [stowaway.mongo :refer [translate-criteria]]))

(derive clojure.lang.PersistentVector ::vector)
(derive clojure.lang.PersistentHashMap ::map)
(derive clojure.lang.PersistentArrayMap ::map)

(defmulti ^:private ->mongodb-sort type)

(defmethod ->mongodb-sort :default
  [x]
  [(->snake_case x) 1])

(defmethod ->mongodb-sort ::vector
  [sort]
  (-> sort
      (update-in [0] ->snake_case)
      (update-in [1] #(if (= :asc %) 1 -1))))

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
