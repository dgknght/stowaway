(ns stowaway.mongo.queries-test
  (:require [clojure.test :refer [deftest is]]
            [stowaway.mongo.queries :as m]))

(deftest convert-simple-criteria-into-a-query
  (is (= {:where {:name "John"}}
         (m/criteria->query {:name "John"}))
      "A simple attribute equalify criterion is left as-is")
  (is (= {:where {:first_name "John"}}
         (m/criteria->query {:first-name "John"}))
      "A kebab-case key is converted to snake case")
  (is (= {:where {:_id 101}}
         (m/criteria->query {:_id 101}))
      "An :_id key is preserved"))

(deftest convert-criterion-with-predicate
  (is (= {:where {:avg_size {:$gt 5}}}
         (m/criteria->query {:avg-size [:> 5]}))
      ":> translates to :$gt")
  (is (= {:where {:avg_size {:$gte 5}}}
         (m/criteria->query {:avg-size [:>= 5]}))
      ":>= translates to :$gte")
  (is (= {:where {:avg_size {:$lt 5}}}
         (m/criteria->query {:avg-size [:< 5]}))
      ":< translates to :$lt")
  (is (= {:where {:avg_size {:$lte 5}}}
         (m/criteria->query {:avg-size [:<= 5]}))
      ":<= translates to :$lte")
  (is (= {:where {:avg_size {:$ne 5}}}
         (m/criteria->query {:avg-size [:!= 5]}))
      ":!= translates to :$ne"))

(deftest convert-compound-criterion
  (is (= {:where {:$or [{:first_name "John"}
                        {:last_name "Doe"}]}}
         (m/criteria->query [:or
                             {:first-name "John"}
                             {:last-name "Doe"}]))
      "A top-level :or is convered correctly")
  (is (= {:where {:first-name "John"
                  :last-name "Doe"}}
        (m/criteria->query [:and
                            {:first-name "John"}
                            {:last-name "Doe"}]))
      "$and is not supported (map is preferred)")
  (is (= {:where {:size {:$gte 2 :$lt 5}}}
         (m/criteria->query {:size [:and [:>= 2] [:< 5]]}))))

(deftest apply-a-sort
  ; NB: These will need to have coerce-ordered-fields applied, but I didn't want
  ; to add the dependency to congomongo here
  (is (= {:sort [[:first_name 1]]}
         (m/criteria->query {} {:sort [[:first-name :asc]]}))
      ":sort is translated")
  (is (= {:sort [[:first_name 1]]}
         (m/criteria->query {} {:order-by [[:first-name :asc]]}))
      ":order-by is is translated as :sort")
  (is (= {:sort [[:first_name -1]]}
         (m/criteria->query {} {:order-by [[:first-name :desc]]}))
      "Order can be descending")
  (is (= {:sort [[:first_name 1]]}
         (m/criteria->query {} {:order-by [:first-name]}))
      "Order is ascending by default"))
