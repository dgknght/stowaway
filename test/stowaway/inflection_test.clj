(ns stowaway.inflection-test
  (:require [clojure.test :refer [deftest are]]
            [stowaway.inflection :as i]))

(deftest make-a-singular-word-plural
  (are [input expected] (= expected (i/plural input))
       "child"  "children"
       :child   :children
       "entity" "entities"
       :entity  :entities
       "bass"   "basses"
       :bass    :basses
       "book"   "books"
       :book    :books))

(deftest make-a-plural-word-singular
  (are [input expected] (= expected (i/singular input))
       "children" "child"
       :children  :child
       "entities" "entity"
       :entities  :entity
       "basses"   "bass"
       :basses    :bass
       "books"    "book"
       :books     :book))
