(ns stowaway.sql
  (:require [clojure.string :as string]
            [camel-snake-kebab.core :refer [->snake_case_string
                                            ->kebab-case-keyword]]
            [honey.sql.helpers :as h]
            [honey.sql :as sql]))

(sql/register-op! (keyword "@>"))
(sql/register-op! (keyword "&&"))

; TODO: move this to an inflection library
(defn- apply-word-rule
  [word {pattern :pattern f :fn}]
  (when-let [match (re-find pattern word)]
    (f match)))

(defn- plural
  [word]
  (let [rules [{:pattern #"(?i)\Achild\z"
                :fn #(str % "ren")}
               {:pattern #"(?i)(.+)s\z"
                :fn #(str (second %) "ses")}
               {:pattern #"(?i)(.+)y\z"
                :fn #(str (second %) "ies")}
               {:pattern #".+"
                :fn #(str % "s")}]]
    (some (partial apply-word-rule word) rules)))

(defn get-int
  [m k]
  (when-let  [v  (get m k)]
    (if (integer? v)
      v
      (try
        (Integer/parseInt v)
        (catch NumberFormatException _
          nil)))))

(defn apply-limit
  [sql options]
  (if-let [limit (get-int options :limit)]
    (h/limit sql limit)
    sql))

(defn apply-offset
  [sql options]
  (if-let [offset (get-int options :offset)]
    (h/offset sql offset)
    sql))

(defn apply-sort
  [sql {:keys [sort]}]
  (if sort
    (apply h/order-by sql sort)
    sql))

(defn select-count
  [sql {:keys [count]}]
  (if count
    (-> sql
        (dissoc :select :order-by)
        (h/select [:%count.1 :record_count]))
    sql))

(defn- ensure-not-keyword
  "Make sure the value is not a keyword. It could be a string, an integer
  or anything else."
  [value]
  (if (keyword? value)
    (name value)
    value))

(defn- col-ref
  [table column]
  (keyword (->> [table column]
                (map ensure-not-keyword)
                (string/join "."))))

(defn- model->table
  [m {:keys [table-names]
      :or {table-names {}}}]
  (get-in table-names [m] (-> m name plural keyword)))

(defn- resolve-join-col
  "Replaces a column spec that references another table with
  a property table.column expression.

  (resolve-join-col [:lot-transaction :lot-id]) => :lots_transactions.lot_id
  {[:lot-transaction :lot-id] (:id lot)}
  [:= :lots_transactions.lot_id (:id lot)]"
  [column-spec options]
  (if (coll? column-spec)
    (let [model (->> column-spec
                     reverse
                     (drop 1)
                     first)]
      (col-ref (model->table model options)
               (->snake_case_string (last column-spec))))
    column-spec))

(defmulti delimit
  type)

(defmethod delimit java.lang.String
  [value]
  (str "\"" value "\""))

(defmethod delimit clojure.lang.Keyword
  [value]
  (str "\"" (name value) "\""))

(defmethod delimit :default
  [value]
  (str value))

(defn- postgres-array
  [values]
  (format "'{%s}'"
          (->> values
               (map delimit)
               (string/join ","))))

(defn- map-entry->statements
  [[k v]]
  (if (coll? v)
    (case (first v)

      (:= :> :>= :<= :< :<> :!= :like)
      [[(first v) k (ensure-not-keyword (second v))]]

      :between
      [[:>= k (ensure-not-keyword (second v))]
       [:<= k (ensure-not-keyword (nth v 2))]]

      :between>
      [[:>= k (ensure-not-keyword (second v))]
       [:< k (ensure-not-keyword (nth v 2))]]

      :<between
      [[:> k (ensure-not-keyword (second v))]
       [:<= k (ensure-not-keyword (nth v 2))]]

      :<between>
      [[:> k (ensure-not-keyword (second v))]
       [:< k (ensure-not-keyword (nth v 2))]]

      :in
      (let [[op values] (if (= :in (first v))
                          v
                          [:in v])]
        [[op k (if (map? values)
                 values ; For now, assume this is a honeysql map. One day we'll make it storage agnostic
                 (map ensure-not-keyword values))]])

      :and
      [(apply vector :and (->> (rest v)
                               (interleave (repeat k))
                               (partition 2)
                               (mapcat map-entry->statements)))]

      :or
      [(apply vector :or (map (comp #(vector := k %)
                                    ensure-not-keyword)
                              (rest v)))]

      :contained-by
      [[(keyword "@>") (second v) k]]

      :any
      [[:= (second v) [:any k]]]

      :&&
      [[(first v) (postgres-array (second v)) k]]

      [[:in k (map ensure-not-keyword v)]])
    [[:= k (ensure-not-keyword v)]]))

(defmulti map->where
  (fn [criteria & _]
    (if (sequential? criteria)
      :clause
      :map)))

(defn- disambiguate-attr
  [attr {:keys [target] :as options}]
  (let [str-attr (name attr)]
    (if (and target
             (not (string/includes? str-attr ".")))
      (keyword (str (name (model->table target options)) "." str-attr))
      attr)))

(defmethod map->where :map
  [m {:keys [prefix] :as options}]
  (let [prefix-fn (if prefix
                    (fn [k]
                      (if (string/includes? (name k) ".")
                        k
                        (keyword
                          (format "%s.%s"
                                  (ensure-not-keyword prefix)
                                  (name k)))))
                    identity)
        result (->> m
                    (map (fn [kv]
                           (update-in kv [0] (comp #(disambiguate-attr % options)
                                                   prefix-fn
                                                   #(resolve-join-col % options)))))
                    (mapcat map-entry->statements))]
    (if (= 1 (count result))
      (first result)
      (concat [:and]
              result))))

(defmethod map->where :clause
  [m options]
  (concat [(first m)]
          (map #(map->where % options)
               (rest m))))

(defmulti ^:private extract-join-keys
  #(if (sequential? %)
     :clause
     :map))

(defmethod ^:private extract-join-keys :clause
  [criteria]
  (mapcat #(extract-join-keys %)
          (rest criteria)))

(defmethod ^:private extract-join-keys :map
  [criteria]
  (->> (keys criteria)
       (filter coll?)))

(defn- relationship
  [rel-key {:keys [relationships]}]
  (when-let [rel (get-in relationships [(set rel-key)])]
    (merge {:primary-id :id} rel)))

(defn- join-cond
  [{:keys [primary-table
           primary-id
           foreign-table
           foreign-id]}]
  (if (sequential? primary-id)
    (->> foreign-id
         (zipmap primary-id)
         (map (fn [[pid fid]]
                [:=
                 (col-ref primary-table pid)
                 (col-ref foreign-table fid)]))
         (into [:and]))
    [:=
     (col-ref primary-table primary-id) ; TODO: this will cause a problem with an alias specified and depth > 1
     (col-ref foreign-table foreign-id)]))

(defn- apply-criteria-join
  [sql rel-key {:keys [target-alias] :as options}]
  (let [existing-joins (->> [:join :left-join :right-join]
                            (mapcat #(get-in sql [%]))
                            (partition 2)
                            (map (comp ->kebab-case-keyword
                                       #(if (vector? %)
                                          (second %)
                                          %)
                                       first))
                            set)
        new-table (model->table (second rel-key) options)
        rel (relationship rel-key options)]
    (if (existing-joins (->kebab-case-keyword new-table))
      sql
      (do
        (assert rel (str "No relationship defined for " (prn-str rel-key)))
        (h/join sql
                new-table
                (join-cond (if target-alias
                             (assoc rel :primary-table target-alias)
                             rel)))))))

(defn- apply-criteria-join-chain
  [sql join-key {:keys [target] :as options}]
  (->> (butlast join-key)
       (concat [target])
       (partition 2 1)
       (reduce #(apply-criteria-join %1 %2 options)
               sql)))

(defn- apply-criteria-joins
  "Creates join clauses for criteria with keys that reference other tables.

  {[:lot-transaction :lot-id] (:id lot)}
  (join sql :lots_transactions [:= :transactions.id :lots_transactions.transaction_id])"
  [sql join-keys options]
  {:pre [(:target options)]}

  (reduce #(apply-criteria-join-chain %1 %2 options)
          sql
          join-keys))

(defn- ensure-criteria-joins
  [sql criteria options]
  (let [join-keys (extract-join-keys criteria)]
    (if (seq join-keys)
      (apply-criteria-joins sql join-keys options)
      sql)))

(defn apply-criteria
  "Adds a WHERE clause to a sql map based on the specified criteria.

  The criteria can be a map, which will result in a set of conditions
  joined by AND.

  (apply-criteria {:first-name \"John\" :last-name \"Doe\"}) => {:where [:and [:= :first_name \"Jone\"] [:= :last_name \"Doe\"]]}

  Maps can be joined with OR logic also.
  [:or {:first-name \"John\"} {:last-name \"Doe\"}] => {:where [:or [:= :first_name \"John\"] [:= :last_name \"Doe\"]]}

  Related tables can be included in a query by passing a :relationships
  map in the options and including a compound key in the criteria.

  (apply-criteria {[:user :last-name] \"Doe\"}
                  {:target :order
                  :relationships {#{:user :order} {:primary-table :users
                                                   :foreign-table :orders
                                                   :foreign-id :user_id}}}) => {:where [:= :users.last_name \"Doe\"]
                                                                                :from :orders
                                                                                :join [:users [:= :users.id :orders.user_id]]}
  If a table name does not match the key used to identify the model type, it will
  first be looked up from the :table-names map in options. Otherwise, an attempt
  will be made to make it plural."
  ([sql criteria]
   (apply-criteria sql criteria {}))
  ([sql criteria options]
   (if (seq criteria)
     (-> sql
         (h/where (map->where criteria options))
         (ensure-criteria-joins criteria options))
     sql)))

(defn- joins-table?
  [{:keys [join]} table]
  (and join
       (->> join
            (partition 2)
            (filter (fn [[t _]]
                      (= table t)))
            seq)))

(defn ensure-join
  "Ensures that the specified sql map contains the specified join clause"
  [sql table criteria]
  (if (joins-table? sql table)
    sql
    (h/join sql table criteria)))

(defn deep-contains?
  "Returns a boolean value indicating whether or not a field
  is specified in a criteria structure.

  (deep-contains? {:first-name \"John\"} :first-name) => true
  (deep-contains? {:last-name \"Doe\"} :first-name) => false
  (deep-contains? [:or {:first-name \"John\"} {:last-name \"Doe\"}] :first-name) => true"
  [data k]
  (cond
    (vector? data) (some #(deep-contains? % k) data)
    (map? data)    (contains? data k)
    :else          false))

(defn deep-get
  "Returns the value specified for a field within a criteria structure.

  (deep-get {:first-name \"John\"} :first-name) => \"John\"
  (deep-get {:last-name \"Doe\"} :first-name) => nil
  (deep-get [:or {:first-name \"John\"} {:last-name \"Doe\"}] :first-name) => \"John\""
  [data k]
  (cond
    (vector? data) (some #(deep-get % k) data)
    (map? data)    (get-in data [k])
    :else          nil))

(defn update-in-if
  [m k-path f]
  (if-let [v (get-in m k-path)]
    (assoc-in m k-path (f v))
    m))

(defn deep-update-in-if
  [data k f]
  (cond
    (vector? data) (mapv #(deep-update-in-if % k f) data)
    (map? data)    (update-in-if data [k] f)
    :else          data))

(defmulti deep-dissoc
  "Removes a value from a criteria structure.

  (deep-dissoc [:or {:first-name \"John\"} {:last-name \"Doe\"}] :first-name) => {:last-name \"Doe\"}"
  (fn [data & _]
    (if (map? data)
      :map
      :clause)))

(defmethod deep-dissoc :map
  [data k]
  (dissoc data k))

(defmethod deep-dissoc :clause
  [data k]
  (let [args (->> (rest data)
                  (map #(deep-dissoc % k))
                  (remove empty?))]
    (if (= 1 (count args))
      (first args)
      (apply vector (first data) args))))
