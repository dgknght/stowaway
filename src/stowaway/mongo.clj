(ns stowaway.mongo
  #_(:require [stowaway.graph :as g]
            [stowaway.inflection :refer [singular]]
            [stowaway.criteria :refer [namespaces
                                       extract-ns]]))

#_(defn- extract-collections
  [criteria]
  (map identity (namespaces criteria)))

#_(defn- match
  [criteria]
  {:$match :x})

#_(defn- lookup-and-match
  [collection relationships])

(defn criteria->aggregation
  [_criteria {:keys [_collection _relationships]}]
  #_(let [paths (g/shortest-paths collection
                                (extract-collections criteria)
                                relationships)]
    (cons (-> criteria
              (extract-ns (singular collection))
              match)
          (map #(lookup-and-match % relationships)
               (rest paths)))))
