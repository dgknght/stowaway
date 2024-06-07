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

(deftest extract-portions-from-a-criteria-map-for-a-namespce
  (is (= {:first-name "John"
          :age 32}
         (c/extract-ns {:user/first-name "John"
                        :user/age 32
                        :order/purchase-date "2010-01-01"}
                       :user))
      "Keys with the specifed ns are retained but stripped of the ns")
  (is (nil? (c/extract-ns {:user/first-name "John"}
                          :order))
      "When no keys match, nil is returned"))

(deftest extract-portions-from-a-criteria-vector-for-a-namespce
  (is (= [:or
          {:first-name "John"}
          {:age 32}]
         (c/extract-ns [:or
                        {:user/first-name "John"
                         :order/purchase-date "2010-01-01"}
                        {:user/age 32}]
                       :user))
      "A conjunction with more than one element is retained")
  (is (= {:first-name "John"}
         (c/extract-ns [:or
                        {:user/first-name "John"}
                        {:order/purchase-date "2010-01-01"}]
                       :user))
      "A single criteria is extracted from a conjunction"))

(deftest extract-a-single-namespace-from-a-criteria
  (is (= "user" (c/single-ns {:user/first-name "John"
                             :user/last-name "Doe"}))
      "When one namespace is found, it is returned")
  (is (nil? (c/single-ns {:first-name "John"}))
      "Wnen no namespaces are found, nil is returned")
  (is (nil? (c/single-ns {:user/first-name "John"
                          :order/purchase-date "2020-01-01"}))
      "When multiple namespaces are found, nil is returned"))
