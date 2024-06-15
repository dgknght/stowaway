(ns stowaway.mongo.pipelines-test
  (:require [clojure.test :refer [deftest is testing]]
            [stowaway.mongo.pipelines :as m]))

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
                                :order/user {:id 101}}
                               {:collection :order-items
                                :relationships #{[:users :orders]
                                                 [:orders :order-items]}}))
      "Keys are also converted in joins"))
