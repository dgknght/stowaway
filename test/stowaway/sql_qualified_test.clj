(ns stowaway.sql-qualified-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.data :refer [diff]]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [honey.sql.helpers :as h]
            [honey.sql :as hsql]
            [stowaway.geometry :as geo]
            [stowaway.sql-qualified :as sql]
            [clojure.pprint :as pp]))

(deftest query-a-single-table-with-simple-single-field-equality
  (is (= ["SELECT users.* FROM users WHERE users.last_name = ?" "Doe"]
         (sql/->query #:user{:last-name "Doe"}))))

(deftest query-a-single-table-by-name
  (is (= ["SELECT users.* FROM users"]
         (sql/->query {} {:target :user}))))

(deftest query-a-single-table-with-explicit-table-name
  (is (= ["SELECT people.* FROM people WHERE people.last_name = ?" "Doe"]
         (sql/->query #:user{:last-name "Doe"}
                      {:table-names {:user :people}}))))

(deftest query-and-specify-a-limit
  (is (= ["SELECT users.* FROM users WHERE users.last_name = ? LIMIT ?" "Doe" 10]
         (sql/->query #:user{:last-name "Doe"}
                      {:limit 10}))
      "A limit is applied when present"))

(deftest query-and-specify-an-offset
  (is (= ["SELECT users.* FROM users WHERE users.last_name = ? OFFSET ?"  "Doe" 20]
         (sql/->query #:user{:last-name "Doe"}
                      {:offset 20}))
      "An offset is applied when present"))

(deftest query-and-sort
  (is (= ["SELECT users.* FROM users WHERE users.last_name = ? ORDER BY users.last_name ASC, users.first_name DESC"
          "Doe"]
         (sql/->query #:user{:last-name "Doe"}
                      {:sort [:user/last-name [:user/first-name :desc]]}))
      "An ORDER BY clauses is added based on the specified :sort value"))

(deftest select-a-count
  (is (= ["SELECT COUNT(1) AS record_count FROM users"]
         (sql/->query {}
                      {:target :user
                       :count true})))
  (testing "sort clauses are ignored"
    (is (= ["SELECT COUNT(1) AS record_count FROM users"]
         (sql/->query {}
                      {:target :user
                       :sort [:user/last-name]
                       :count true})))))

(deftest query-against-multiple-simple-equality-criteria
  (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) AND (users.age = ?)"
          "John"
          25]
         (sql/->query #:user{:first-name "John"
                             :age 25}))))

(deftest query-against-a-union-of-multiple-equality-criteria
  (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) OR (users.age = ?)"
          "John"
          25]
         (sql/->query [:or
                       {:user/first-name "John"}
                       {:user/age 25}]))))

(deftest query-against-a-union-of-values-for-a-single-field
  (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) OR (users.first_name = ?)"
          "Jane"
          "John"]
         (sql/->query {:user/first-name [:or "Jane" "John"]}))))

(deftest query-against-comparison-operators
  (is (= ["SELECT users.* FROM users WHERE (users.age >= ?) AND (users.age < ?)"
          1
          5]
         (sql/->query {:user/age [:and
                                  [:>= 1]
                                  [:< 5]]}))))

(deftest query-against-a-join-with-inferred-primary-key
  (is (= ["SELECT users.* FROM users INNER JOIN addresses ON users.id = addresses.user_id WHERE addresses.city = ?"
          "Dallas"]
         (sql/->query {:address/city "Dallas"}
                      {:target :user
                       :relationships #{[:users :addresses]}}))))

(deftest query-against-a-two-step-join
  (is (= ["SELECT users.* FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN line_items ON orders.id = line_items.order_id WHERE (line_items.sku = ?) AND (orders.order_date = ?)"
          "ABC123"
          "2020-01-01"]
         (sql/->query {:line-item/sku "ABC123"
                       :order/order-date "2020-01-01"}
                      {:target :user
                       :relationships #{[:users :orders]
                                        [:orders :line_items]}}))))

(deftest query-against-an-implicit-intermediate-join
  (is (= ["SELECT users.* FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN line_items ON orders.id = line_items.order_id WHERE line_items.sku = ?"
          "ABC123"]
         (sql/->query {:line-item/sku "ABC123"}
                      {:target :user
                       :relationships #{[:users :orders]
                                        [:users :addresses]
                                        [:orders :line_items]}}))))

(deftest apply-complex-criteria
  (is (= ["SELECT users.* FROM users WHERE ((users.first_name = ?) OR (users.first_name = ?)) AND ((users.age >= ?) AND (users.age <= ?) AND (users.size IN (?, ?, ?)))"
          "John"
          "Jane"
          20
          30
          2 3 4]
         (sql/->query [:and
                       [:or
                        {:user/first-name "John"}
                        {:user/first-name "Jane"}]
                       {:user/age [:between 20 30]
                        :user/size [:in '(2 3 4)]}]))))

(deftest apply-criteria-with-explicit-join-expression
  (is (= ["SELECT settings.* FROM settings INNER JOIN users ON (users.id = settings.owner_id) AND (settings.owner_type = ?) WHERE users.last_name = ?"
          "user"
          "Doe"]
         (sql/->query {:user/last-name "Doe"}
                      {:target :setting
                       :relationships #{[:users :settings]}
                       :joins {[:users :settings]
                               [:and
                                [:= :users.id :settings.owner_id]
                                [:= :settings.owner_type "user"]]}}))))

(deftest apply-criteria-to-array-field
  (is (= ["SELECT orders.* FROM orders WHERE ? && orders.tags" "'{\"rush\",\"preferred\"}'"]
         (sql/->query {:order/tags [:&& #{:rush :preferred}]}))))

#_(deftest specify-an-outer-join
  (is (= ["SELECT orders.* FROM orders LEFT JOIN users ON users.id = orders.user_id WHERE users.first_name = ?" "Doug"]
         (sql/->query {:user/first-name "Doug"}
                      {:target :order
                       :join-hints {:order :all}
                       :relationships #{[:user :order]}}))))

#_(deftest apply-criteria-with-sub-query
  (let [subquery (-> (h/select :organization_id)
                     (h/from :memberships)
                     (h/where [:= :user_id 123]))
        actual (-> (h/select :*)
                   (h/from :organizations)
                   (sql/apply-criteria {:id [:in subquery]})
                   hsql/format)
        expected ["SELECT * FROM organizations WHERE id IN (SELECT organization_id FROM memberships WHERE user_id = ?)"
                  123]]
    (when-not (= expected actual)
      (pprint (diff expected actual)))
    (is (= expected actual))))

#_(deftest apply-criteria-with-join-on-compound-key
  (let [actual (-> (h/select :transactions.*)
                   (h/from :transactions)
                   (sql/apply-criteria {:attachmeht/id 101}
                                       :target :transaction
                                       :relationships {#{:transaction :attachment} {:primary-table :transactions
                                                                                    :foreign-table :attachments
                                                                                    :primary-id [:transaction_date :id]
                                                                                    :foreign-id [:transaction_date :transaction_id]}})
                   hsql/format)
        expected [(string/join
                    " "
                    ["SELECT transactions.*"
                     "FROM transactions"
                     "INNER JOIN attachments"
                     "ON (transactions.transaction_date = attachments.transaction_date)"
                     "AND (transactions.id = attachments.transaction_id)"
                     "WHERE attachments.id = ?"])
                  101]]
    (is (= expected actual))))

#_(deftest test-for-deeply-contained-key
  (is (sql/deep-contains? {:one 1} :one))
  (is (sql/deep-contains? [:and {:one 1}] :one)))

#_(deftest find-deeply-contained-value
  (is (= 1 (sql/deep-get {:one 1} :one)))
  (is (= 1 (sql/deep-get [:and {:one 1}] :one))))

#_(deftest deeply-dissoc-a-value
  (is (= {:first-name "John"}
         (sql/deep-dissoc {:first-name "John"
                           :last-name "Doe"}
                          :last-name))
      "A map is treated like dissoc")
  (is (= {:first-name "John"}
         (sql/deep-dissoc [:or
                           {:first-name "John"}
                           {:last-name "Doe"}]
                          :last-name))
      "A redundant clause is removed"))

#_(deftest update-a-value-if-it-exists
  (is (= {:age 26}
         (sql/update-in-if {:age 25} [:age] inc))
      "The value is updated if it is present")
  (is (= {}
         (sql/update-in-if {} [:age] inc))
      "The value is not added if it is not present"))

#_(deftest update-a-vlaue-in-a-criteria-if-it-exists
  (is (= {:age 26
          :size "large"}
         (sql/update-in-if {:age 25
                            :size "large"}
                           [:age]
                           inc))
      "The value is updated if it is present")
  (is (= [:or {:color "blue"} {:size "large"}]
         (sql/update-in-if [:or {:color "blue"} {:size "large"}]
                           [:age]
                           inc))
      "The value is not added if it is not present"))

(deftest query-against-a-point
  (is (= ["SELECT locations.* FROM locations WHERE ? @> locations.center" (geo/->Circle (geo/->Point 2 2) 3)]
         (sql/->query {:location/center [:contained-by :?geoloc]}
                      {:named-params {:geoloc (geo/->Circle (geo/->Point 2 2) 3)}}))))
