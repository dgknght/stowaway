(ns stowaway.criteria-test
  (:require [clojure.test :refer [deftest is are testing assert-expr do-report]]
            [clojure.spec.alpha :as s]
            [stowaway.criteria :as c]))

(defmethod assert-expr 'valid?
  [msg form]
  `(let [spec# ~(nth form 1)
         value# ~(nth form 2)]
     (do-report {:type (if (s/valid? spec# value#) :pass :fail)
                 :expected :valid
                 :actual (s/explain-str spec# value#)
                 :message ~msg})))

(deftest extract-namespaces-from-a-criteria-map
  (testing "as strings (default)"
    (is (= #{"user" "order"}
           (c/namespaces {:user/first-name "John"
                          :order/purchase-date "2010-01-01"}))))
  (testing "as keys"
    (is (= #{:user :order}
           (c/namespaces {:user/first-name "John"
                          :order/purchase-date "2010-01-01"}
                         :as-keywords true)))))

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

(deftest validate-a-criteria
  (is (valid? ::c/criteria {:user/name "John"})
      "A map with a simple equality check is valid")
  (is (valid? ::c/criteria {:user/name [:!= "John"]})
      "A map with a vector specifying a predicate is valid")
  (is (not (s/valid? ::c/criteria {:user/name ["Jim" "John"]}))
      "A map with a vector of values is not valid.")
  (is (valid? ::c/criteria {:user/name [:in ["Jim" "John"]]})
      "A map with a vector of values wrapped in a vector with a predicate is valid.")
  (is (valid? ::c/criteria {:order/user {:id 101}})
      "A map with a simple model/entity reference is valid")
  (is (not (s/valid? ::c/criteria {:order/user {:first-name "John"}}))
      "A map with a value that is a map but not a model/entity reference is not valid")
  (is (valid? ::c/criteria [:or
                              {:order/user {:id 101}}
                              {:user/last-name "Doe"}])
      "A vector starting with :or and containing value criteria maps is valid")
  (is (not (s/valid? ::c/criteria [:fish
                                   {:order/user {:id 101}}
                                   {:user/last-name "Doe"}]))
      "A vector starting with an unrecognized conjunction is not valid")
  (is (not (s/valid? ::c/criteria [{:order/user {:id 101}}
                                   {:user/last-name "Doe"}]))
      "A vector of maps is not valid")
  (is (valid? ::c/criteria {})
      "An empty map is valid")
  (is (not (s/valid? ::c/criteria []))
      "An empty vector is valid"))

(deftest simplify-a-redundant-and
  (is (= {:first-name "John"
          :last-name "Doe"}
         (c/simplify-and
           [:and
            {:first-name "John"}
            {:last-name "Doe"}]))
      "A redundant and is simplified")
  (is (not (c/simplify-and
             [:and
              {:first-name "John"}
              {:first-name "Jane"}]))
      "Duplicate keys cannot be simplified")
  (is (not (c/simplify-and
             [:and
              {:last-name "Doe"}
              [:or
               {:age 21}
               {:first-name "Jane"}]]))
      "Nesting ors cannot be simplified"))
