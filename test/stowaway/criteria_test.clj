(ns stowaway.criteria-test
  (:require [clojure.test :refer [deftest is]]
            [stowaway.criteria :as c]))

(deftest extract-namespaces-from-a-criteria-map
  (is (= #{"user" "order"}
         (c/namespaces {:user/first-name "John"
                        :order/purchase-date "2010-01-01"}))))

(deftest extract-namespaces-from-a-criteria-vector
  (is (= #{"user" "order"}
         (c/namespaces [:or
                        {:user/first-name "John"}
                        {:order/purchase-date "2010-01-01"}]))))
