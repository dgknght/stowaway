(ns stowaway.mongo-test
  (:require [clojure.test :refer [deftest is testing]]
            [stowaway.mongo :as m]))

(deftest convert-criteria-into-an-aggregation-pipeline
  (testing "simple map with downstream join"
    (is (= [{:$match {:first_name "John"}}
          {:$lookup {:from "orders"
                    :as "orders"
                    :localField "_id"
                    :foreignField "user_id"}}
            {:$match {:orders.purchase_date "2020-01-01"}}]
         (m/criteria->aggregation {:order/purchase-date "2020-01-01"
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
         (m/criteria->aggregation {:order/purchase-date "2020-01-01"
                                   :user/first-name "John"}
                                  {:collection :orders
                                   :relationships #{[:users :orders]}})))))
