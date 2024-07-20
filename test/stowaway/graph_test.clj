(ns stowaway.graph-test
  (:require [clojure.test :refer [deftest is testing]]
            [stowaway.graph :as g]))

(deftest get-the-shortest-paths-to-connect-a-set-of-nodes
  (testing "Single path"
    (is (= [[:order :user]]
           (g/shortest-paths :order #{:user}
                             :relationships #{[:user :order]}))))
  (testing "Two steps"
    (is (= [[:order-item :order :user]]
           (g/shortest-paths :order-item #{:user}
                             :relationships #{[:user :order]
                                              [:order :order-item]}))))
  (testing "Two paths"
    (is (= [[:order-item :order :user]
            [:order-item :product]]
           (g/shortest-paths :order-item #{:user :order :product}
                             :relationships #{[:user :order]
                                              [:order :order-item]
                                              [:product :order-item]}))))
    (testing "No path found"
      (is (empty? (g/shortest-paths :order-item #{:popsicle}
                                    :relationships #{[:order :order-item]})))))
