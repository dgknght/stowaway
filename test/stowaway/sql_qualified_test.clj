(ns stowaway.sql-qualified-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [stowaway.geometry :as geo]
            [stowaway.inflection :refer [plural]]
            [stowaway.sql-qualified :as sql]))

; Common criteria 1: single field match
; #:user{:last-name "Doe"}
(deftest query-a-single-table-with-simple-single-field-equality
  (testing "singular table name"
    (is (= ["SELECT user.* FROM user WHERE user.last_name = ?" "Doe"]
         (sql/->query #:user{:last-name "Doe"}))))
  (testing "plural table name"
    (is (= ["SELECT users.* FROM users WHERE users.last_name = ?" "Doe"]
         (sql/->query #:user{:last-name "Doe"}
                      {:table-fn plural})))))

(deftest query-a-single-table-by-name
  (is (= ["SELECT user.* FROM user"]
         (sql/->query {} {:target :user}))))

(deftest query-a-single-table-with-explicit-table-name
  (is (= ["SELECT people.* FROM people WHERE people.last_name = ?" "Doe"]
         (sql/->query #:user{:last-name "Doe"}
                      {:table-names {:user :people}}))))

(deftest query-and-specify-a-limit
  (is (= ["SELECT user.* FROM user WHERE user.last_name = ? LIMIT ?" "Doe" 10]
         (sql/->query #:user{:last-name "Doe"}
                      {:limit 10}))
      "A limit is applied when present"))

(deftest query-and-specify-an-offset
  (is (= ["SELECT user.* FROM user WHERE user.last_name = ? OFFSET ?"  "Doe" 20]
         (sql/->query #:user{:last-name "Doe"}
                      {:offset 20}))
      "An offset is applied when present"))

(deftest query-and-sort
  (is (= ["SELECT user.* FROM user WHERE user.last_name = ? ORDER BY user.last_name ASC, user.first_name DESC"
          "Doe"]
         (sql/->query #:user{:last-name "Doe"}
                      {:sort [:user/last-name [:user/first-name :desc]]}))
      "An ORDER BY clauses is added based on the specified :sort value"))

(deftest select-a-count
  (testing "unquoted"
    (is (= ["SELECT COUNT(*) AS record_count FROM user"]
           (sql/->query {}
                        {:target :user
                         :count true})))
    (testing "sort clauses are ignored"
      (is (= ["SELECT COUNT(*) AS record_count FROM user"]
             (sql/->query {}
                          {:target :user
                           :sort [:user/last-name]
                           :count true})))))
  (testing "quoted"
    (is (= ["SELECT COUNT(*) AS \"record_count\" FROM \"user\""]
           (sql/->query {}
                        {:target :user
                         :count true
                         :quoted? true})))
    (testing "sort clauses are ignored"
      (is (= ["SELECT COUNT(*) AS \"record_count\" FROM \"user\""]
             (sql/->query {}
                          {:target :user
                           :sort [:user/last-name]
                           :count true
                           :quoted? true}))))))

; Common criteria 2: model id
; {:user/id "101"}
(deftest query-against-a-simple-id
  (is (= ["SELECT user.* FROM user WHERE user.id = ?"
          "101"]
         (sql/->query {:user/id "101"})))
  (is (= ["SELECT user.* FROM user WHERE user.id = ?"
          101]
         (sql/->query {:user/id "101"}
                      {:coerce-id #(Integer/parseInt %)}))
      "An id can be coerced"))

; Common criteria 3: query with a predicate
; {:user/id [:!= "101"]}
(deftest apply-id-criterion-with-predicate
  (is (= ["SELECT user.* FROM user WHERE user.id <> ?"
          "101"]
         (sql/->query {:user/id [:!= "101"]}))))

; Common criteria 4: multiple simple equality criteria
; #:user{:first-name "John"
;        :age 25}
(deftest query-against-multiple-simple-equality-criteria
  (is (= ["SELECT user.* FROM user WHERE (user.first_name = ?) AND (user.age = ?)"
          "John"
          25]
         (sql/->query #:user{:first-name "John"
                             :age 25}))))

; Common criteria 5: model reference
; {:order/user {:id 101}}
(deftest query-against-criteria-with-a-model-reference
  (testing "a scalar id value"
    (is (= ["SELECT order.* FROM order WHERE order.user_id = ?"
            101]
           (sql/->query {:order/user {:id 101}}))))
  (testing "an :in clause"
    (is (= ["SELECT order.* FROM order WHERE order.user_id IN (?, ?)", 1, 2]
         (sql/->query {:order/user [:in [{:id 1} {:id 2}]]})))))

; Common criteria 6: subquery against attributes
; {:user/identity [:including {:identity/oauth-provider "google" :identity/oauth-id "abc123"}]}
(deftest query-against-subquery-criteria
  (is (= ["SELECT user.* FROM user WHERE id IN (SELECT identity.user_id FROM identity WHERE (identity.oauth_provider = ?) AND (identity.oauth_id = ?))"
          "google"
          "abc123"]
         (sql/->query {:user/identity [:including
                                       #:identity{:oauth-provider "google"
                                                  :oauth-id "abc123"}]}))))

; Common criteria 7: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (testing "redundant"
    (is (= ["SELECT user.* FROM user WHERE (user.first_name = ?) AND (user.age = ?)"
            "John"
            25]
           (sql/->query [:and
                         {:user/first-name "John"}
                         {:user/age 25}]))))
  (testing "unmatchable"
    (is (= ["SELECT user.* FROM user WHERE (user.first_name = ?) AND (user.first_name = ?)"
            "John"
            "Jane"]
           (sql/->query [:and
                         {:user/first-name "John"}
                         {:user/first-name "Jane"}])))))

; Common criteria 8: "or" conjunction
; [:or {:user/first-name "John"} {:user/age 25}]
(deftest query-against-a-union-of-multiple-equality-criteria
  (is (= ["SELECT user.* FROM user WHERE (user.first_name = ?) OR (user.age = ?)"
          "John"
          25]
         (sql/->query [:or
                       {:user/first-name "John"}
                       {:user/age 25}]))))

; Common criteria 9: complex conjunction
; [:and [:or {:user/first-name "John"} {:user/age 25}] {:user/last-name "Doe"}]
(deftest query-against-a-complex-conjunction
  (is (= ["SELECT user.* FROM user WHERE ((user.first_name = ?) OR (user.age = ?)) AND (user.last_name = ?)"
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
  (is (= ["SELECT transaction.* FROM transaction WHERE (transaction.date >= ?) AND (transaction.date <= ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:between "2020-01-01" "2020-12-31"]}))
      ":between is inclusive on both ends")
  (is (= ["SELECT transaction.* FROM transaction WHERE (transaction.date > ?) AND (transaction.date <= ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:<between "2020-01-01" "2020-12-31"]}))
      ":<between is exclusive of the lower bound and inclusive of the upper")
  (is (= ["SELECT transaction.* FROM transaction WHERE (transaction.date >= ?) AND (transaction.date < ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:between> "2020-01-01" "2020-12-31"]}))
      ":between> is inclusive of the lower bound and exclusive of the upper")
  (is (= ["SELECT transaction.* FROM transaction WHERE (transaction.date > ?) AND (transaction.date < ?)"
          "2020-01-01"
          "2020-12-31"]
         (sql/->query #:transaction{:date [:<between> "2020-01-01" "2020-12-31"]}))
      ":<between> is exclusive of the lower bound and exclusive of the upper"))

(deftest query-against-a-union-of-values-for-a-single-field
  (is (= ["SELECT user.* FROM user WHERE (user.first_name = ?) OR (user.first_name = ?)"
          "Jane"
          "John"]
         (sql/->query {:user/first-name [:or "Jane" "John"]}))))

(deftest query-against-comparison-operators
  (is (= ["SELECT user.* FROM user WHERE (user.age >= ?) AND (user.age < ?)"
          1
          5]
         (sql/->query {:user/age [:and
                                  [:>= 1]
                                  [:< 5]]}))))

(deftest query-against-a-join-with-inferred-primary-key
  (is (= ["SELECT user.* FROM user INNER JOIN address ON user.id = address.user_id WHERE address.city = ?"
          "Dallas"]
         (sql/->query {:address/city "Dallas"}
                      {:target :user
                       :relationships #{[:user :address]}}))))

(deftest query-against-a-two-step-join
  (is (= ["SELECT user.* FROM user INNER JOIN order ON user.id = order.user_id INNER JOIN line_item ON order.id = line_item.order_id WHERE (line_item.sku = ?) AND (order.order_date = ?)"
          "ABC123"
          "2020-01-01"]
         (sql/->query {:line-item/sku "ABC123"
                       :order/order-date "2020-01-01"}
                      {:target :user
                       :relationships #{[:user :order]
                                        [:order :line-item]}}))))

(deftest query-against-an-implicit-intermediate-join
  (is (= ["SELECT user.* FROM user INNER JOIN order ON user.id = order.user_id INNER JOIN line_item ON order.id = line_item.order_id WHERE line_item.sku = ?"
          "ABC123"]
         (sql/->query {:line-item/sku "ABC123"}
                      {:target :user
                       :relationships #{[:user :order]
                                        [:user :address]
                                        [:order :line-item]}}))))

(deftest apply-complex-criteria
  (is (= ["SELECT user.* FROM user WHERE ((user.first_name = ?) OR (user.first_name = ?)) AND ((user.age >= ?) AND (user.age <= ?) AND (user.size IN (?, ?, ?)))"
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
  (is (= ["SELECT setting.* FROM setting INNER JOIN user ON (user.id = setting.owner_id) AND (setting.owner_type = ?) WHERE user.last_name = ?"
          "user"
          "Doe"]
         (sql/->query {:user/last-name "Doe"}
                      {:target :setting
                       :relationships #{[:user :setting]}
                       :joins {[:user :setting]
                               [:and
                                [:= :user.id :setting.owner_id]
                                [:= :setting.owner_type "user"]]}}))))

(deftest apply-criteria-to-array-field
  (is (= ["SELECT order.* FROM order WHERE ? && order.tags" "'{\"rush\",\"preferred\"}'"]
         (sql/->query {:order/tags [:&& #{:rush :preferred}]}))))

(deftest specify-an-outer-join
  (is (= [(string/join
            " "
            ["SELECT order.*"
             "FROM order"
             "LEFT JOIN user ON user.id = order.user_id WHERE user.first_name = ?"])
          "Doug"]
         (sql/->query {:user/first-name "Doug"}
                      {:target :order
                       :full-results #{:order}
                       :relationships #{[:user :order]}}))))

(deftest apply-criteria-with-sub-query
  (is (= [(string/join
            " "
            ["SELECT organization.*"
             "FROM organization"
             "WHERE organization.id IN"
             "(SELECT membership.organization_id FROM membership WHERE membership.user_id = ?)"])
          123]
         (sql/->query {:organization/id [:in '({:membership/user_id 123}
                                               {:select [:membership/organization-id]})]}))))

(deftest apply-criteria-with-join-on-compound-key
  (is (= [(string/join
            " "
            ["SELECT transaction.*"
             "FROM transaction"
             "INNER JOIN attachment"
             "ON (transaction.id = attachment.transaction_id)"
             "AND (transaction.transaction_date = attachment.transaction_date)"
             "WHERE attachment.id = ?"])
          101]
         (sql/->query {:attachment/id 101}
                      {:target :transaction
                       :relationships #{[:transaction :attachment]}
                       :joins {[:transaction :attachment]
                               [:and
                                [:= :transaction.id :attachment.transaction_id]
                                [:= :transaction.transaction_date :attachment.transaction_date]]}}))))

(deftest query-against-a-point
  (is (= ["SELECT location.* FROM location WHERE ? @> location.center"
          (geo/->Circle (geo/->Point 2 2) 3)]
         (sql/->query {:location/center [:contained-by :?geoloc]}
                      {:named-params {:geoloc (geo/->Circle (geo/->Point 2 2) 3)}}))))

(deftest specify-select-clause
  (is (= ["SELECT transaction_item.quantity FROM transaction_item WHERE transaction_item.reconciliation_id = ?"
          101]
         (sql/->query {:transaction-item/reconciliation {:id 101}}
                      {:select :transaction-item/quantity}))
      "The entire select clause can be specified")
  (is (= ["SELECT id FROM transaction_item WHERE transaction_item.reconciliation_id = ?"
          101]
         (sql/->query {:transaction-item/reconciliation {:id 101}}
                      {:select :id}))
      "The select clause can include keywords without namespaces")
  (is (= ["SELECT transaction_item.quantity, transaction_item.value FROM transaction_item WHERE transaction_item.reconciliation_id = ?"
          101]
         (sql/->query {:transaction-item/reconciliation {:id 101}}
                      {:select [:transaction-item/quantity
                                :transaction-item/value]}))
      "The entire select clause can be specified")
  (is (= ["SELECT transaction_item.*, transaction.description FROM transaction_item INNER JOIN transaction ON transaction.id = transaction_item.transaction_id WHERE transaction_item.reconciliation_id = ?"
          101]
         (sql/->query {:transaction-item/reconciliation {:id 101}}
                      {:select-also :transaction/description
                       :relationships #{[:transaction :transaction-item]}}))
      "An additional select column can be specified")
  (is (= ["SELECT transaction_item.*, transaction.description, transaction.memo FROM transaction_item INNER JOIN transaction ON transaction.id = transaction_item.transaction_id WHERE transaction_item.reconciliation_id = ?"
          101]
         (sql/->query {:transaction-item/reconciliation {:id 101}}
                      {:select-also [:transaction/description
                                     :transaction/memo]
                       :relationships #{[:transaction :transaction-item]}}))
      "Multiple additional select columns can be specified"))

; WITH raccount AS (
;     SELECT account.*
;     FROM account
;     WHERE account.name = ?
;   UNION
;     SELECT account.*
;     FROM account
;     INNER JOIN raccount
;     ON account.parent_id = raccount.id
; )
; SELECT raccount.*
; FROM raccount
(deftest query-with-recursion
  (is (= ["WITH RECURSIVE cte AS (SELECT account.* FROM account WHERE account.name = ? UNION SELECT account.* FROM account INNER JOIN cte ON account.parent_id = cte.id) SELECT cte.* FROM cte"
          "Checking"]
         (sql/->query {:account/name "Checking"}
                      {:recursion [:parent-id :id]}))))

(deftest handle-keyword-values
  (is (= ["SELECT user.* FROM user WHERE user.first_name = ?", "Jane"]
         (sql/->query {:user/first-name :Jane}))
      "A non-namespaced keyword is coverted to a string")
  (is (= ["SELECT user.* FROM user WHERE user.first_name IN (?, ?)", "Jane", "John"]
         (sql/->query {:user/first-name [:in [:Jane :John]]}))))

(deftest update-multiple-rows
  (testing "single table update"
    (is (= ["UPDATE user SET last_name = ? WHERE (user.first_name IN (?, ?)) AND (user.age = ?)"
            "Doe"
            "Jane"
            "John"
            30]
           (sql/->update {:user/last-name "Doe"}
                         {:user/first-name [:in ["Jane" "John"]]
                          :user/age 30}))))
  (testing "update a table with a where clause that joins"
    (is (= ["UPDATE order SET discount = ? FROM order AS x INNER JOIN user ON user.id = x.user_id WHERE user.first_name IN (?, ?)"
            0.1M
            "Jane"
            "John"]
           (sql/->update {:order/discount 0.1M}
                         {:user/first-name [:in ["Jane" "John"]]}
                         :relationships #{[:user :order]})))))

(deftest avoid-duplicate-table-references
  ; This commented one is really better, but it was only happening by accident, as
  ; graph library was arbitrarily choosing between two paths of the same length.
  ; One day maybe we can work out a way to get the first one reliably.
  (is (= [#_"SELECT transaction_item.* FROM transaction_item INNER JOIN account ON account.id = transaction_item.account_id INNER JOIN entity ON entity.id = account.entity_id INNER JOIN reconciliation ON account.id = reconciliation.account_id WHERE (((transaction_item.transaction_date >= ?) AND (transaction_item.transaction_date < ?) AND (transaction_item.account_id = ?)) AND ((transaction_item.reconciliation_id IS NULL) OR (reconciliation.status = ?))) AND (entity.user_id = ?) ORDER BY transaction_item.index DESC LIMIT ?"
          "SELECT transaction_item.* FROM transaction_item INNER JOIN transaction ON transaction.id = transaction_item.transaction_id INNER JOIN entity ON entity.id = transaction.entity_id INNER JOIN account ON account.id = transaction_item.account_id INNER JOIN reconciliation ON account.id = reconciliation.account_id WHERE (((transaction_item.transaction_date >= ?) AND (transaction_item.transaction_date < ?) AND (transaction_item.account_id = ?)) AND ((transaction_item.reconciliation_id IS NULL) OR (reconciliation.status = ?))) AND (entity.user_id = ?) ORDER BY transaction_item.index DESC LIMIT ?"
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
            :relationships #{[:user :entity]
                             [:entity :account]
                             [:entity :transaction]
                             [:account :transaction-item]
                             [:account :reconciliation]
                             [:transaction :transaction-item]}}))))
