(ns stowaway.mongo-test
  (:require [clojure.test :refer [deftest is]]
            [stowaway.mongo :as m]))

(deftest convert-a-criteria-into-an-aggregation-pipeline
  (is (= [{:$match {:purchase_date "2020-01-01"}}
          {:$lookup {:from "users"
                    :as "users"
                    :localField "user_id"
                    :foreignField "_id"}}
          {:$match {:users.first_name "John"}}]
         (m/criteria->aggregation {:order/purchase-date "2020-01-01"
                                   :user/first-name "John"}
                                  {:target :user
                                   :relationships #{[:users :orders]}}))))
