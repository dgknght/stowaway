(ns stowaway.mongo.pipelines-test
  (:require [clojure.test :refer [deftest is testing]]
            [stowaway.mongo.pipelines :as m]))

(deftest split-on-namespace
  ; 1. get the list of matching entities
  ; 2. update the commodity query to include entity ids from 1st query
  (is (= [{:$match {:entity_id 201}} ; 1st match is against the target collection, commodities
          {:$lookup {:from "entities"
                     :localField "entity_id"
                     :foreignField "_id"
                     :as "entities"}}
          ; in this direction, should we call the lookup "entity" and unwind it?
          {:$match {:entities.owner_id 101}}]
         (m/criteria->pipeline {:commodity/entity-id 201
                               :entity/owner-id 101}
                              {:target :commodity
                               :relationships #{[:user :entity]
                                                [:entity :commodity]}}))))

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
