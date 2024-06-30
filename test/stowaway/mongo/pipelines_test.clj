(ns stowaway.mongo.pipelines-test
  (:require [clojure.test :refer [deftest is testing]]
            [stowaway.mongo.pipelines :as m]))

; Common criteria 1: single field match
; #:user{:last-name "Doe"}
(deftest convert-a-single-field-match-criteria
  (is (= [{:$match {:last_name "Doe"}}]
         (m/criteria->pipeline {:user/last-name "Doe"}
                               {:collection :users}))))

; Common criteria 2: model id
; {:user/id "101"}
(deftest query-against-a-simple-id
  (is (= [{:$match {:_id "101"}}]
         (m/criteria->pipeline {:user/id "101"})))
  (is (= [{:$match {:_id 101}}]
         (m/criteria->pipeline {:user/id "101"}
                               {:coerce-id #(Integer/parseInt %)}))
      "An id can be coerced"))

; Common criteria 3: predicate
; {:user/id [:!= "101"]}
(deftest query-against-a-predicate
  (is (= [{:$match {:_id {:$ne "101"}}}]
         (m/criteria->pipeline {:user/id [:!= "101"]}))))

; Common criteria 4: multiple simple equality criteria
; #:user{:first-name "John"
;        :age 25}
(deftest query-multiple-simple-equality-criteria
  (is (= [{:$match {:first_name "John"
                    :age 25}}]
         (m/criteria->pipeline #:user{:first-name "John"
                                      :age 25}))))

; Common criteria 5: model reference
; {:order/user {:id 101}}
(deftest query-against-criteria-with-a-model-reference
  (is (= [{:$match {:user_id 101}}]
         (m/criteria->pipeline {:order/user {:id 101}}))))

; Common criteria 6: subquery against attributes
; {:user/identities [:including {:identity/oauth-provider "google" :identity/oauth-id "abc123"}]}
(deftest query-against-subquery-criteria
  (is (= [{:$match {:identities {:$elemMatch {:oauth_provider "google"
                                              :oauth_id "abc123"}}}}]
         (m/criteria->pipeline {:user/identities [:including
                                                  #:identity{:oauth-provider "google"
                                                             :oauth-id "abc123"}]}))))

; Common criteria 7: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (is (= [{:$match {:first_name "John"
                    :age 25}}]
         (m/criteria->pipeline [:and
                                {:user/first-name "John"}
                                {:user/age 25}]))))

; Common criteria 8: "or" conjunction
; [:or {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (is (= [{:$match {:$or [{:first_name "John"}
                          {:age 25}]}}]
         (m/criteria->pipeline [:or
                                {:user/first-name "John"}
                                {:user/age 25}]))))

; Common criteria 9: complex conjunction
; [:and [:or {:user/first-name "John"} {:user/age 25}] {:user/last-name "Doe"}]
(deftest query-against-a-complex-conjunction
  (is (= [{:$match {:and [{:$or [{:first_name "John"}
                                 {:age 25}]}
                          {:last_name "Doe"}]}}]
         (m/criteria->pipeline [:and
                                [:or
                                 {:user/first-name "John"}
                                 {:user/age 25}]
                                {:user/last-name "Doe"}]))))

(deftest convert-a-criteria-to-an-aggregation-pipeline
  (testing "upstream join"
    (is (= [{:$match {:purchase_date "2020-01-01"}}
            {:$lookup {:from "users"
                       :localField "user_id"
                       :foreignField "_id"
                       :as "users"}}
            ; in this direction, should we call the lookup "user" and unwind it?
            {:$match {:users.first_name "John"}}]
           (m/criteria->pipeline {:user/first-name "John"
                                  :order/purchase-date "2020-01-01"}
                                 {:collection :orders
                                  :relationships #{[:users :orders]}}))))
  (testing "downstream join"
    (is (= [{:$match {:first_name "John"}}
            {:$lookup {:from "orders"
                       :localField "_id"
                       :foreignField "user_id"
                       :as "orders"}}
            {:$match {:orders.purchase_date "2020-01-01"}}]
           (m/criteria->pipeline {:user/first-name "John"
                                  :order/purchase-date "2020-01-01"}
                                 {:collection :users
                                  :relationships #{[:users :orders]}})))))

(deftest convert-criteria-into-an-aggregation-pipeline
  (testing "simple map with downstream join"
    (is (= [{:$match {:first_name "John"}}
            {:$lookup {:from "orders"
                       :as "orders"
                       :localField "_id"
                       :foreignField "user_id"}}
            {:$match {:orders.purchase_date "2020-01-01"}}]
           (m/criteria->pipeline {:order/purchase-date "2020-01-01"
                                  :user/first-name "John"}
                                 {:collection :users
                                  :relationships #{[:users :orders]}}))))
  ; by "upstream", we mean that the relationship is defined
  ; users -> orders, or a user has many orders and an order belongs
  ; to a user. However, the target collection is :orders, so navigating
  ; the graph to users goes against the direction of the primary -> foreign
  ; relationship
  (testing "simple map with upstream join"
    (is (= [{:$match {:purchase_date "2020-01-01"}}
            {:$lookup {:from "users"
                       :as "users"
                       :localField "user_id"
                       :foreignField "_id"}}
            {:$match {:users.first_name "John"}}]
           (m/criteria->pipeline {:order/purchase-date "2020-01-01"
                                  :user/first-name "John"}
                                 {:collection :orders
                                  :relationships #{[:users :orders]}})))))

(deftest convert-criteria-with-negative-match-to-an-aggregation-pipeline
  (is (= [{:$match {:age {:$ne 21}}}]
         (m/criteria->pipeline {:user/age [:!= 21]}
                               {:collection :users}))))

(deftest convert-criteria-for-count
  (testing "single collection"
    (is (= [{:$match {:first_name "John"}}
            {:$count "document_count"}]
           (m/criteria->pipeline {:users/first-name "John"}
                                 {:count true}))))
  (testing "with join"
    (is (= [{:$match {:first_name "John"}}
            {:$lookup {:from "orders"
                       :as "orders"
                       :localField "_id"
                       :foreignField "user_id"}}
            {:$match {:orders.purchase_date "2020-01-01"}}
            {:$count "document_count"}]
           (m/criteria->pipeline {:user/first-name "John"
                                  :order/purchase-date "2020-01-01"}
                                 {:collection :users
                                  :relationships #{[:users :orders]}
                                  :count true})))))

(deftest convert-criteria-with-id-key
  (is (= [{:$match {:_id 101}}]
         (m/criteria->pipeline {:user/_id 101}))))

(deftest convert-criteria-with-model-reference
  (is (= [{:$match {:entity_id 101}}]
         (m/criteria->pipeline {:commodity/entity-id 101}))
      "The foreign key is transformed into snake case")
  (is (= [{:$match {:entity_id 101}}]
         (m/criteria->pipeline {:commodity/entity {:id 101}}))
      "A simplified reference is converted to a foreign key")
  (is (= [{:$match {:sku "ABC123"}}
          {:$lookup {:from "orders"
                     :as "orders"
                     :localField "order_id"
                     :foreignField "_id"}}
          {:$match {:orders.user_id 101}}]
         (m/criteria->pipeline {:order-item/sku "ABC123"
                                :order/user {:id "101"}}
                               {:collection :order-items
                                :coerce-id #(Integer/parseInt %)
                                :relationships #{[:users :orders]
                                                 [:orders :order-items]}}))
      "Keys are also converted in joins"))

(deftest convert-criteria-with-a-limit
  (is (= [{:$match {:symbol "USD"
                    :entity_id 101}}
          {:$limit 1}]
         (m/criteria->pipeline {:commodity/symbol "USD",
                                :commodity/entity {:id "101"}}
                               {:limit 1
                                :collection :commodities
                                :coerce-id #(Integer/parseInt %)}))))
