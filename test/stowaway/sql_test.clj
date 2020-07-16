(ns stowaway.sql-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.data :refer [diff]]
            [clojure.pprint :refer [pprint]]
            [honeysql.helpers :as h]
            [honeysql.format :as f]
            [stowaway.sql :as sql]))

(deftest apply-a-limit
  (is (= ["SELECT * FROM users LIMIT ?", 10]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-limit {:limit 10})
             f/format))
      "A limit is applied when present")
  (is (= ["SELECT * FROM users"]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-limit {:offset 20})
             f/format))
      "A limit is not applied if absent"))

(deftest apply-an-offset
  (is (= ["SELECT * FROM users OFFSET ?", 20]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-offset {:offset 20})
             f/format))
      "An offset is applied when present")
  (is (= ["SELECT * FROM users"]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-offset {:limit 10})
             f/format))
      "An offset is not applied if absent"))

(deftest apply-sort-to-a-query
  (is (= ["SELECT * FROM users ORDER BY last_name, first_name DESC"]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-sort {:sort [:last_name [:first_name :desc]]})
             f/format)))
  (is (= ["SELECT * FROM users"]
         (-> (h/select :*)
             (h/from :users)
             (sql/apply-sort {})
             f/format))))

(deftest select-a-count
  (is (= ["SELECT count(1) AS record_count FROM users"]
         (-> (h/select :*)
             (h/from :users)
             (sql/select-count {:count true})
             f/format)))
  (is (= ["SELECT * FROM users"]
         (-> (h/select :*)
             (h/from :users)
             (sql/select-count {})
             f/format))))

(deftest apply-criteria-to-sql
  (is (= {:where [:= :name "John"]}
         (sql/apply-criteria {} {:name "John"}))
      "A single-field equality can be added")
  (is (= {:where [:and
                  [:= :name "John"]
                  [:= :age 25]]}
         (sql/apply-criteria {} {:name "John"
                                 :age 25}))
      "Two single-field equalities can be joined with 'and'")
  (is (= {:where [:or
                  [:= :name "John"]
                  [:= :age 25]]}
         (sql/apply-criteria {} [:or
                                 {:name "John"}
                                 {:age 25}]))
      "Two single-field equalities can be joined with 'or'")
  (is (= {:where [:or
                  [:= :name nil]
                  [:= :name "John"]]}
         (sql/apply-criteria {} {:name [:or nil "John"]}))
      "Multiple values can be joined with :or for a single field")
  (let [expected {:where [:= :addresses.city "Dallas"]
                  :join [:addresses [:= :users.id :addresses.user_id]]}
        actual (sql/apply-criteria {}
                                   {[:address :city] "Dallas"}
                                   {:target :user
                                    :relationships {#{:user :address} {:primary-table :users
                                                                       :foreign-table :addresses
                                                                       :foreign-id    :user_id}}})]
    (when-not (= expected actual)
      (pprint (diff expected actual)))
    (is (= expected actual) "A join can be infered"))
  (let [actual (-> (h/select :first_name)
                   (h/from :users)
                   (sql/apply-criteria [:and
                                        [:or
                                         {:first-name "John"}
                                         {:first-name "Jane"}]
                                        {:age [:between 20 30]
                                         :size [:in '(2 3 4)]}])
                   f/format)
        expected ["SELECT first_name FROM users WHERE ((first_name = ? OR first_name = ?) AND (age >= ? AND age <= ? AND (size in (?, ?, ?))))"
            "John"
            "Jane"
            20
            30
            2 3 4]]
    (when-not (= expected actual)
      (pprint (diff expected actual)))
    (is (= expected actual)
        "A complex criteria structure is mapped correctly.")))

(deftest an-existing-join-is-not-duplicated
  (is (= ["SELECT * FROM orders INNER JOIN users ON users.id = orders.user_id WHERE users.first_name = ?" "Doug"]
         (-> (h/select :*)
             (h/from :orders)
             (h/join :users [:= :users.id :orders.user_id])
             (sql/apply-criteria {[:user :first_name] "Doug"}
                                 {:target :order
                                  :relationships {#{:order :user} {:primary-table :users
                                                                   :foreign-table :orders
                                                                   :foreign-id :user_id}}})
             f/format))
      "An inner join is not duplicated")
  (is (= ["SELECT * FROM orders LEFT JOIN users ON users.id = orders.user_id WHERE users.first_name = ?" "Doug"]
         (-> (h/select :*)
             (h/from :orders)
             (h/left-join :users [:= :users.id :orders.user_id])
             (sql/apply-criteria {[:user :first_name] "Doug"}
                                 {:target :order
                                  :relationships {#{:order :user} {:primary-table :users
                                                                   :foreign-table :orders
                                                                   :foreign-id :user_id}}})
             f/format))
      "An left join is not duplicated")
  (is (= ["SELECT * FROM orders RIGHT JOIN users ON users.id = orders.user_id WHERE users.first_name = ?" "Doug"]
         (-> (h/select :*)
             (h/from :orders)
             (h/right-join :users [:= :users.id :orders.user_id])
             (sql/apply-criteria {[:user :first_name] "Doug"}
                                 {:target :order
                                  :relationships {#{:order :user} {:primary-table :users
                                                                   :foreign-table :orders
                                                                   :foreign-id :user_id}}})
             f/format))
      "An right join is not duplicated"))

(deftest apply-criteria-with-sub-query
  (let [subquery (-> (h/select :organization_id)
                     (h/from :memberships)
                     (h/where [:= :user_id 123]))
        actual (-> (h/select :*)
                   (h/from :organizations)
                   (sql/apply-criteria {:id [:in subquery]})
                   f/format)
        expected ["SELECT * FROM organizations WHERE (id in (SELECT organization_id FROM memberships WHERE user_id = ?))"
                  123]]
    (when-not (= expected actual)
      (pprint (diff expected actual)))
    (is (= expected actual))))

(deftest apply-criteria-with-join-on-compound-key
  (let [actual (-> (h/select :transactions.*)
                   (h/from :transactions)
                   (sql/apply-criteria {[:attachment :id] 101}
                                       {:target :transaction
                                        :relationships {#{:transaction :attachment} {:primary-table :transactions
                                                                                     :foreign-table :attachments
                                                                                     :primary-id [:transaction_date :id]
                                                                                     :foreign-id [:transaction_date :transaction_id]}}})
                   f/format)
        expected ["SELECT transactions.* FROM transactions INNER JOIN attachments ON (transactions.transaction_date = attachments.transaction_date AND transactions.id = attachments.transaction_id) WHERE attachments.id = ?"
                  101]]
    (is (= expected actual))))

(deftest test-for-deeply-contained-key
  (is (sql/deep-contains? {:one 1} :one))
  (is (sql/deep-contains? [:and {:one 1}] :one)))

(deftest find-deeply-contained-value
  (is (= 1 (sql/deep-get {:one 1} :one)))
  (is (= 1 (sql/deep-get [:and {:one 1}] :one))))

(deftest deeply-dissoc-a-value
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

(deftest update-a-value-if-it-exists
  (is (= {:age 26}
         (sql/update-in-if {:age 25} [:age] inc))
      "The value is updated if it is present")
  (is (= {}
         (sql/update-in-if {} [:age] inc))
      "The value is not added if it is not present"))

(deftest update-a-vlaue-in-a-criteria-if-it-exists
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
