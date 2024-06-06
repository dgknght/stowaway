(ns stowaway.util-test
  (:require [clojure.test :refer [deftest is]]
            [stowaway.util :as u]))

(deftest conditionally-update-a-map
  (is (= {:name "John"
          :age 26}
         (u/update-in-if {:name "John"
                          :age 25}
                         [:age] inc))
      "The value is updated if it is present")
  (is (= {:user {:name "John"
                 :age 26}}
         (u/update-in-if {:user {:name "John"
                                 :age 25}}
                         [:user :age] inc))
      "A nested map is updated.")
  (is (= {}
         (u/update-in-if {} [:age] inc))
      "The action is ignored if the attributes is absent"))

(deftest qualify-map-keys
  (is (= {:entity/name "Personal"}
         (u/qualify-keys {:name "Personal"} :entity))
      "Unqualified keys are qualified with the model type")
  (is (= {:db/id "x"}
         (u/qualify-keys {:db/id "x"} :entity))
      "Qualified keys are left as-is")
  (is (= {:id 101
          :user/name "John"}
         (u/qualify-keys {:id 101 :name "John"}
                           :user
                           :ignore #{:id}))
      "Keys can be explicitly ignored"))

(deftest unqaulify-map-keys
  (is (= {:name "Personal"}
         (u/unqualify-keys {:entity/name "Personal"}))))
