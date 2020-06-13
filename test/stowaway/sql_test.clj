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
    (is (= expected actual))))

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
