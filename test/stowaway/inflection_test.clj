(ns stowaway.inflection-test
  (:require [clojure.test :refer [deftest are]]
            [stowaway.inflection :as i]))

(deftest make-a-singular-word-plural
  (are [input expected] (= expected (i/plural input))
       "child"  "children"
       "entity" "entities"
       "bass"   "basses"
       "book"   "books"))

(deftest make-a-plural-word-singular
  (are [input expected] (= expected (i/singular input))
       "children" "child"
       "entities" "entity"
       "basses"   "bass"
       "books"    "book"))
