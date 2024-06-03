(ns stowaway.mongo
  (:require [clojure.pprint :refer [pprint]]
            [camel-snake-kebab.core :refer [->snake_case_keyword]]
            [stowaway.graph :as g]
            [stowaway.inflection :refer [singular
                                         plural]]
            [stowaway.criteria :refer [namespaces
                                       extract-ns]]))

(defn- extract-collections
  "Give a criteria, return a vector of namespaces tranlated
  into collection names as keywords"
  [criteria]
  (map (comp keyword
             plural)
       (namespaces criteria)))

(defn- match
  [criteria]
  {:$match (update-keys criteria ->snake_case_keyword)})

(defn- lookup-and-match
  [[n1 n2 :as edge] relationships]
  (let [[_ c2] (some relationships
                      [edge
                       (reverse edge)])
        from (name n2)
        [local foreign] (if (= c2 n2)
                          ["_id" (str (name n1) "_id")]
                          [(str (singular (name n2)) "_id") "_id"])]
    [{:$lookup {:from from
               :as from
               :localField local
               :foreignField foreign}}]))

(defn- path->stages
  [path relationships]
  (->> path
       (partition 2 1)
       (mapcat #(lookup-and-match % relationships))))

(defn criteria->aggregation
  [criteria {:keys [collection relationships]}]
  (let [paths (g/shortest-paths collection
                                (extract-collections criteria)
                                relationships)]
    (cons (-> criteria
              (extract-ns (singular collection))
              match)
          (mapcat #(path->stages % relationships)
               paths))))
