(ns stowaway.graph
  (:require [clojure.pprint :refer [pprint]]
            [ubergraph.core :as g]
            [ubergraph.alg :as ga]))

(defn- shortest-path
  [graph from to]
  (ga/nodes-in-path
    (ga/shortest-path graph from to)))

(defn- shortest-path-fn
  [from {:keys [relationships graph]}]
  (let [graph (or graph
                  (apply g/graph relationships))]
    (comp #(shortest-path graph from %)
          keyword)))

(defn- starts-with?
  [p1 p2]
  (= (take (count p2) p1)
     p2))

(defn- drop-duplicative
  [ps p]
  (if (some #(starts-with? % p)
            ps)
    ps
    (conj ps p)))

(defn shortest-paths
  "Given a set of nodes, return the list of shortest paths that
  connects all of them."
  [from nodes & {:as opts}]
  (->> nodes
       (map (shortest-path-fn from opts))
       (filter identity)
       (sort-by count >)
       (reduce drop-duplicative [])))
