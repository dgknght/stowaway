(ns stowaway.mongo.pipelines
  (:require [clojure.pprint :refer [pprint]]
            [camel-snake-kebab.core :refer [->snake_case_keyword]]
            [stowaway.graph :as g]
            [stowaway.inflection :refer [singular
                                         plural]]
            [stowaway.criteria :refer [namespaces
                                       extract-ns
                                       single-ns]]
            [stowaway.mongo :refer [translate-criteria]]))

(defn- extract-collections
  "Give a criteria, return a vector of namespaces tranlated
  into collection names as keywords"
  [criteria]
  (map (comp keyword
             plural)
       (namespaces criteria)))

(defn- ->snake-case-keyword
  [x]
  (if (= :_id x)
    x
    (->snake_case_keyword x)))

(defn- match
  [criteria & [prefix]]
  (let [prefix-fn (if prefix
                    #(keyword (str (name prefix) "." (name %)))
                    identity)]
    {:$match (update-keys criteria (comp ->snake-case-keyword
                                         prefix-fn))}))

(defn- ref-field
  "Given a collection name, return the name of the field another
  collection would use to reference a document in the given collection.

  E.g., given the collection name :users, return :user_id."
  [collection]
  (str (singular (name collection)) "_id"))

(defn- lookup-and-match
  [[_ n2 :as edge] criteria {:keys [relationships] :as options}]
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
     (-> criteria
         (extract-ns (singular n2))
         (translate-criteria options)
         (match from)) ]))

(defn- path->stages
  [path criteria options]
  (->> path
       (partition 2 1)
       (mapcat #(lookup-and-match % criteria options))))

(defn- assert-readiness
  [collection targets paths {:keys [relationships]}]
  (assert (or (= 1 (count (conj (set targets)
                                   collection)))
                 (seq paths))
             (format "Unable to connect the target collection %s to all elements of the criteria %s via relationships %s"
                     collection
                     (into [] targets)
                     relationships)))

(defn- calc-paths
  [criteria {:keys [collection relationships] :as options}]
  (let [targets (extract-collections criteria)
        paths (g/shortest-paths collection
                                targets
                                relationships)]
    (assert-readiness collection targets paths options)
    paths))

(defn criteria->pipeline
  ([criteria] (criteria->pipeline criteria {}))
  ([criteria {:keys [count collection] :as options}]
   (let [collection (or collection
                       (some-> criteria
                               single-ns
                               plural
                               keyword)
                       (throw (RuntimeException. "Unable to determine the target location for the criteria.")))
         paths (calc-paths criteria (assoc options :collection collection))
         first-stage (some-> criteria
                             (extract-ns (singular collection))
                             (translate-criteria options)
                             match)
         remaining-stages (mapcat #(path->stages % criteria options)
                                  paths)
         stages (cons first-stage remaining-stages)]
     (if count
       (concat stages [{:$count "document_count"}])
       stages))))
