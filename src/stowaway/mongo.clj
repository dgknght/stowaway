(ns stowaway.mongo
  (:require [stowaway.graph :as g]
            [stowaway.criteria :refer [namespaces]]))

(defn- extract-collections
  [criteria]
  (map identity (namespaces criteria)))

(defn criteria->aggregation
  [criteria {:keys [collection relationships]}]
  (let [paths (g/shortest-paths collection
                                (extract-collections criteria)
                                relationships)]))
