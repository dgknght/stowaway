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

(defn- append-targets
  [{:keys [criteria] :as m}]
  (assoc m :targets (extract-collections criteria)))

(defn- single-col-query?
  [{:keys [targets collection]}]
  (= 1 (count (conj (set targets) collection))))

(defn- append-paths
  [{:keys [collection targets relationships] :as m}]
  (if (single-col-query? m)
    m
    (if-let [paths (seq (g/shortest-paths collection
                                          targets
                                          relationships))]
      (assoc m :paths paths)
      (throw (ex-info "Unable to connect the target collection to all elements of the criteria" m)))))

(defn- ensure-collection
  [{:keys [collection criteria] :as m}]
  (if collection
    m
    (if-let [col (some-> criteria
                         single-ns
                         plural
                         keyword)]
      (assoc m :collection col)
      (throw (ex-info "Unable to determine the target collection for the criteria." m)))))

(defn- calc-stages
  [{:keys [criteria collection paths] :as m}]
  (cons (some-> criteria
                (extract-ns (singular collection))
                (translate-criteria m)
                match)
        (mapcat #(path->stages % criteria m)
                paths)))

(defn- append-stages
  [m]
  (assoc m :stages (calc-stages m)))

(defn- append-count-stage
  [{:keys [count] :as m}]
  (if count
    (update-in m [:stages] concat [{:$count "document_count"}])
    m))

(defn- append-limit-stage
  [{:keys [limit] :as m}]
  (if limit
    (update-in m [:stages] concat [{:$limit limit}])
    m))

(defn criteria->pipeline
  ([criteria] (criteria->pipeline criteria {}))
  ([criteria options]
   (-> options
       (assoc :criteria criteria)
       ensure-collection
       append-targets
       append-paths
       append-stages
       append-limit-stage
       append-count-stage
       :stages)))
