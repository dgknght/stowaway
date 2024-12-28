(ns stowaway.sql-qualified-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [stowaway.geometry :as geo]
            [stowaway.sql-qualified :as sql]))

; Common criteria 1: single field match
; #:user{:last-name "Doe"}
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

; Common criteria 2: model id
; {:user/id "101"}
(deftest query-against-a-simple-id
  (is (= ["SELECT users.* FROM users WHERE users.id = ?"
          "101"]
         (sql/->query {:user/id "101"})))
  (is (= ["SELECT users.* FROM users WHERE users.id = ?"
          101]
         (sql/->query {:user/id "101"}
                      {:coerce-id #(Integer/parseInt %)}))
      "An id can be coerced"))

; Common criteria 3: query with a predicate
; {:user/id [:!= "101"]}
(deftest apply-id-criterion-with-predicate
  (is (= ["SELECT users.* FROM users WHERE users.id <> ?"
          "101"]
         (sql/->query {:user/id [:!= "101"]}))))

; Common criteria 4: multiple simple equality criteria
; #:user{:first-name "John"
;        :age 25}
(deftest query-against-multiple-simple-equality-criteria
  (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) AND (users.age = ?)"
          "John"
          25]
         (sql/->query #:user{:first-name "John"
                             :age 25}))))

; Common criteria 5: model reference
; {:order/user {:id 101}}
(deftest query-against-criteria-with-a-model-reference
  (is (= ["SELECT orders.* FROM orders WHERE orders.user_id = ?"
          101]
         (sql/->query {:order/user {:id 101}}))))

; Common criteria 6: subquery against attributes
; {:user/identities [:including {:identity/oauth-provider "google" :identity/oauth-id "abc123"}]}
(deftest query-against-subquery-criteria
  (is (= ["SELECT users.* FROM users WHERE id IN (SELECT identities.user_id FROM identities WHERE (identities.oauth_provider = ?) AND (identities.oauth_id = ?))"
          "google"
          "abc123"]
         (sql/->query {:user/identities [:including
                                         #:identity{:oauth-provider "google"
                                                    :oauth-id "abc123"}]}))))

; Common criteria 7: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (testing "redundant"
    (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) AND (users.age = ?)"
            "John"
            25]
           (sql/->query [:and
                         {:user/first-name "John"}
                         {:user/age 25}]))))
  (testing "unmatchable"
    (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) AND (users.first_name = ?)"
            "John"
            "Jane"]
           (sql/->query [:and
                         {:user/first-name "John"}
                         {:user/first-name "Jane"}])))))

; Common criteria 8: "or" conjunction
; [:or {:user/first-name "John"} {:user/age 25}]
(deftest query-against-a-union-of-multiple-equality-criteria
  (is (= ["SELECT users.* FROM users WHERE (users.first_name = ?) OR (users.age = ?)"
          "John"
          25]
         (sql/->query [:or
                       {:user/first-name "John"}
                       {:user/age 25}]))))

; Common criteria 9: complex conjunction
; [:and [:or {:user/first-name "John"} {:user/age 25}] {:user/last-name "Doe"}]
(deftest query-against-a-complex-conjunction
  (is (= ["SELECT users.* FROM users WHERE ((users.first_name = ?) OR (users.age = ?)) AND (users.last_name = ?)"
          "John"
          25
          "Doe"]
         (sql/->query [:and
                       [:or
                        {:user/first-name "John"}
                        {:user/age 25}]
                       {:user/last-name "Doe"}]))))

; Common criteria 10: between predicates
; {:transaction/date [:between "2020-01-01" "2020-02-01"]}
(deftest convert-a-between-operator
  (is (= ["SELECT transactions.* FROM transactions WHERE (transactions.date >= ?) AND (transactions.date <= ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:between "2020-01-01" "2020-12-31"]}))
      ":between is inclusive on both ends")
  (is (= ["SELECT transactions.* FROM transactions WHERE (transactions.date > ?) AND (transactions.date <= ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:<between "2020-01-01" "2020-12-31"]}))
      ":<between is exclusive of the lower bound and inclusive of the upper")
  (is (= ["SELECT transactions.* FROM transactions WHERE (transactions.date >= ?) AND (transactions.date < ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:between> "2020-01-01" "2020-12-31"]}))
      ":between> is inclusive of the lower bound and exclusive of the upper")
  (is (= ["SELECT transactions.* FROM transactions WHERE (transactions.date > ?) AND (transactions.date < ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:<between> "2020-01-01" "2020-12-31"]}))
      ":<between> is exclusive of the lower bound and exclusive of the upper"))

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
          4 3 2]
         (sql/->query [:and
                       [:or
                        {:user/first-name "John"}
                        {:user/first-name "Jane"}]
                       {:user/age [:between 20 30]
                        :user/size [:in #{2 3 4}]}]))))

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

(deftest specify-an-outer-join
  (is (= [(string/join
            " "
            ["SELECT orders.*"
             "FROM orders"
             "LEFT JOIN users ON users.id = orders.user_id WHERE users.first_name = ?"])
          "Doug"]
         (sql/->query {:user/first-name "Doug"}
                      {:target :order
                       :full-results #{:order}
                       :relationships #{[:users :orders]}}))))

(deftest apply-criteria-with-sub-query
  (is (= [(string/join
            " "
            ["SELECT organizations.*"
             "FROM organizations"
             "WHERE organizations.id IN"
             "(SELECT memberships.organization_id FROM memberships WHERE memberships.user_id = ?)"])
          123]
         (sql/->query {:organization/id [:in '({:membership/user_id 123}
                                               {:select [:membership/organization-id]})]}))))

(deftest apply-criteria-with-join-on-compound-key
  (is (= [(string/join
            " "
            ["SELECT transactions.*"
             "FROM transactions"
             "INNER JOIN attachments"
             "ON (transactions.id = attachments.transaction_id)"
             "AND (transactions.transaction_date = attachments.transaction_date)"
             "WHERE attachments.id = ?"])
          101]
         (sql/->query {:attachment/id 101}
                      {:target :transaction
                       :relationships #{[:transactions :attachments]}
                       :joins {[:transactions :attachments]
                               [:and
                                [:= :transactions.id :attachments.transaction_id]
                                [:= :transactions.transaction_date :attachments.transaction_date]]}}))))

(deftest query-against-a-point
  (is (= ["SELECT locations.* FROM locations WHERE ? @> locations.center"
          (geo/->Circle (geo/->Point 2 2) 3)]
         (sql/->query {:location/center [:contained-by :?geoloc]}
                      {:named-params {:geoloc (geo/->Circle (geo/->Point 2 2) 3)}}))))

; WITH raccounts AS (
;     SELECT accounts.*
;     FROM accounts
;     WHERE accounts.name = ?
;   UNION
;     SELECT accounts.*
;     FROM accounts
;     INNER JOIN raccounts
;     ON accounts.parent_id = raccounts.id
; )
; SELECT raccounts.*
; FROM raccounts
(deftest query-with-recursion
  (is (= ["WITH RECURSIVE cte AS (SELECT accounts.* FROM accounts WHERE accounts.name = ? UNION SELECT accounts.* FROM accounts INNER JOIN cte ON accounts.parent_id = cte.id) SELECT cte.* FROM cte"
          "Checking"]
         (sql/->query {:account/name "Checking"}
                      {:recursion [:parent-id :id]}))))

(deftest handle-keyword-values
  (is (= ["SELECT users.* FROM users WHERE users.first_name = ?", "Jane"]
         (sql/->query {:user/first-name :Jane}))
      "A non-namespaced keyword is coverted to a string")
  (is (= ["SELECT users.* FROM users WHERE users.first_name IN (?, ?)", "Jane", "John"]
         (sql/->query {:user/first-name [:in [:Jane :John]]}))))

(deftest update-multiple-rows
  (testing "single table update"
    (is (= ["UPDATE users SET last_name = ? WHERE (users.first_name IN (?, ?)) AND (users.age = ?)"
            "Doe"
            "Jane"
            "John"
            30]
           (sql/->update {:user/last-name "Doe"}
                         {:user/first-name [:in ["Jane" "John"]]
                          :user/age 30}))))
  (testing "update a table with a where clause that joins"
    (is (= ["UPDATE orders SET discount = ? FROM orders AS x INNER JOIN users ON users.id = x.user_id WHERE users.first_name IN (?, ?)"
            0.1M
            "Jane"
            "John"]
           (sql/->update {:order/discount 0.1M}
                         {:user/first-name [:in ["Jane" "John"]]}
                         :relationships #{[:users :orders]})))))

(deftest avoid-duplicate-table-references
  (is (= ["SELECT transaction_items.* FROM transaction_items INNER JOIN accounts ON accounts.id = transaction_items.account_id INNER JOIN entities ON entities.id = accounts.entity_id INNER JOIN reconciliations ON accounts.id = reconciliations.account_id WHERE (((transaction_items.transaction_date >= ?) AND (transaction_items.transaction_date < ?) AND (transaction_items.account_id = ?)) AND ((transaction_items.reconciliation_id IS NULL) OR (reconciliations.status = ?))) AND (entities.user_id = ?) ORDER BY transaction_items.index DESC LIMIT ?"
          "2017-01-01"
          "2017-01-31"
          77248
          "new"
          21484
          5]
         (sql/->query
           [:and
            [:and
             #:transaction-item{:transaction-date [:between> "2017-01-01" "2017-01-31"],
                                :account {:id 77248}}
             [:or
              #:transaction-item{:reconciliation {:id nil}}
              #:reconciliation{:status "new"}]]
            #:entity{:user {:id 21484}}]
           {:limit 5
            :target :transaction-item
            :sort [[:transaction-item/index :desc]]
            :relationships #{[:users :entities]
                             [:entities :accounts]
                             [:entities :transactions]
                             [:accounts :transaction_items]
                             [:accounts :reconciliations]
                             [:transactions :transaction_items]}}))))
