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
  [criteria & [prefix]]
  (let [prefix-fn (if prefix
                    #(keyword (str (name prefix) "." (name %)))
                    identity)]
    {:$match (update-keys criteria (comp ->snake_case_keyword
                                         prefix-fn))}))

(defn- ref-field
  "Given a collection name, return the name of the field another
  collection would use to reference a document in the given collection.

  E.g., given the collection name :users, return :user_id."
  [collection]
  (str (singular (name collection)) "_id"))

(defn- lookup-and-match
  [[_ n2 :as edge] criteria relationships]
  (let [[c1 c2] (some relationships
                      [edge
                       (reverse edge)])
        from (name n2)
        ref-field (ref-field c1)
        [local foreign] (if (= c2 n2)
                          ["_id" ref-field]
                          [ref-field "_id"])]
    [{:$lookup {:from from
               :as from
               :localField local
               :foreignField foreign}}
     (match (extract-ns criteria, (singular n2))
            from)]))

(defn- path->stages
  [path criteria relationships]
  (->> path
       (partition 2 1)
       (mapcat #(lookup-and-match % criteria relationships))))

(defn criteria->aggregation
  [criteria {:keys [collection relationships]}]
  (let [paths (g/shortest-paths collection
                                (extract-collections criteria)
                                relationships)]
    (cons (-> criteria
              (extract-ns (singular collection))
              match)
          (mapcat #(path->stages % criteria relationships)
               paths))))
