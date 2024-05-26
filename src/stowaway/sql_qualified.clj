(ns stowaway.sql-qualified
  (:require [clojure.string :as str]
            [stowaway.sql :as sql]
            [honey.sql.helpers :as h]
            [camel-snake-kebab.core :refer [->snake_case]]))

(def apply-limit sql/apply-limit)
(def apply-offset sql/apply-offset)
(def select-count sql/select-count)
(def deep-get sql/deep-get)
(def deep-contains? sql/deep-contains?)
(def deep-dissoc sql/deep-dissoc)
(def update-in-if sql/update-in-if)
(def plural sql/plural)

(defn- ->col-ref
  "Accepts a qualified keyword and returns a column reference
  in the form of table_name.column_name"
  [k {:keys [table-names]
      :or {table-names {}}}]
  (let [field (name k)
        model (namespace k)
        table-name (get-in table-names [model] (-> model name plural ->snake_case))]
    (keyword (->> [table-name field]
                  (filter identity)
                  (str/join ".")))))

(defn- normalize-sort-spec
  [sort-spec]
  (if (vector? sort-spec)
    sort-spec
    [sort-spec :asc]))

(defn apply-sort
  [sql {:keys [sort] :as opts}]
  (if sort
    (apply h/order-by sql (map (comp (fn [s]
                                       (update-in s [0] #(->col-ref % opts)))
                                     normalize-sort-spec)
                               sort))
    sql))

(defn apply-criteria
  [sql _criteria & {:as _opts}]
  sql)
