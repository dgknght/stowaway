(ns stowaway.datalog-test
  (:require [clojure.test :refer [deftest testing is]]
            [stowaway.datalog :as dtl]))

(def ^:private query '{:find [?x]})

; Common criteria 1: single field match
; #:user{:last-name "Doe"}
(deftest apply-a-simple-criterion
  (is (= '{:find [?x]
           :where [[?x :user/last-name ?a]]
           :in [?a]
           :args ["Doe"]}
         (dtl/apply-criteria query
                             #:user{:last-name "Doe"}))))

(deftest apply-a-simple-criterion-and-specify-the-entity-var
  (is (= '{:find [?usr]
           :where [[?usr :user/first-name ?a]]
           :in [?a]
           :args ["John"]}
         (dtl/apply-criteria '{:find [?usr]}
                             #:user{:first-name "John"}
                             {:entity-ref '?usr}))))

; Common criteria 2: model id
; {:id "101"}
(deftest apply-a-simple-id-criterion
  (testing "with implicit target entity"
    (is (= '{:find [(pull ?x [*])]
             :where [[?x :entity/name ?name]]
             :in [?x]
             :args [101]}
           (dtl/apply-criteria '{:find [(pull ?x [*])]
                                 :where [[?x :entity/name ?name]]}
                               {:id 101}))))
  (testing "with explicit target entity"
    (is (= '{:find [(pull ?x [*])]
             :where [[?x :entity/name ?name]]
             :in [?x]
             :args [101]}
           (dtl/apply-criteria '{:find [(pull ?x [*])]
                                 :where [[?x :entity/name ?name]]}
                               {:id "101"}
                               {:target :entity})))))

; Common criteria 3: query with a predicate
; {:id [:!= "101"]}
(deftest apply-id-criterion-with-predicate
  (is (= '{:find [(pull ?x [*])]
           :where [[?x :entity/name ?name]
                   [(!= ?x ?a)]]
           :in [?a]
           :args [101]}
         (dtl/apply-criteria '{:find [(pull ?x [*])]
                               :where [[?x :entity/name ?name]]}
                             {:id [:!= "101"]})))
  (is (= '{:find [(pull ?x [*])]
           :where [[?x :user/email ?a]
                   [(!= ?x ?b)]]
           :in [?a ?b]
           :args ["john@doe.com" 101]}
         (dtl/apply-criteria '{:find [(pull ?x [*])]}
                             {:user/email "john@doe.com"
                              :id [:!= "101"]}))))

; Common criteria 4: multiple simple equality criteria
; #:user{:first-name "John"
;        :age 25}
(deftest apply-multiple-simple-equality-criteria
  (is (= '{:find [?x]
           :where [[?x :user/first-name ?a]
                   [?x :user/age ?b]]
           :in [?a ?b]
           :args ["John" 25]}
         (dtl/apply-criteria query
                             #:user{:first-name "John"
                                    :age 25}))))

; Common criteria 5: model reference
; {:order/user {:id 101}}
(deftest query-against-criteria-with-a-model-reference
  (is (= '{:find [?x]
           :where [[?x :order/user ?a]]
           :in [?a]
           :args [101]}
         (dtl/apply-criteria query
                             {:order/user {:id 101}}))))

; Common criteria 6: subquery against attributes
; {:user/identities [:including-match {:identity/oauth-provider "google" :identity/oauth-id "abc123"}]}
(deftest query-against-subquery-criteria
  (is (= '{:find [?x]
           :where [[?x :user/identities ?identity]
                   [?identity :identity/oauth-provider ?a]
                   [?identity :identity/oauth-id ?b]]
           :in [?a ?b]
           :args ["google" "abc123"]}
         (dtl/apply-criteria query
                             {:user/identities [:including-match
                                                #:identity{:oauth-provider "google"
                                                           :oauth-id "abc123"}]}))))

(deftest query-specified-tuple-against-a-collection-of-types
  (is (= '{:find [?x]
           :where [[?x :user/identities ?a]]
           :in [?a]
           :args [["google" "abc123"]]}
         (dtl/apply-criteria query
                             {:user/identities [:including
                                                ["google" "abc123"]]}))))

; TODO: Revisit this, as the equals should probably not be used for an insclusion test
(deftest apply-a-tuple-matching-criterion
  ; here it's necessary to use the := operator explicitly so that
  ; the query logic doesn't mistake :google for the operator
  (is (= '{:find [?x]
           :where [[?x :user/identities ?a]]
           :in [?a]
           :args [[:google "abc123"]]}
         (dtl/apply-criteria query
                             #:user{:identities [:= [:google "abc123"]]}))))

; Common criteria 7: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (testing "redundant"
    (is (= '{:find [?x]
             :where [[?x :user/first-name ?a]
                     [?x :user/age ?b]]
             :in [?a ?b]
             :args ["John" 25]}
           (dtl/apply-criteria query
                               [:and
                                {:user/first-name "John"}
                                {:user/age 25}])))
    (is (= '{:find [?x]
             :where [[?x :user/first-name ?a]]
             :in [?a]
             :args ["John"]}
           (dtl/apply-criteria query
                               [:and
                                {:user/first-name "John"}
                                {:user/first-name "John"}]))))
  (testing "unmatchable"
    (is (= '{:find [?x]
             :where [[?x :user/first-name ?a]
                     [?x :user/first-name ?b]]
             :in [?a ?b]
             :args ["John" "Jane"]}
           (dtl/apply-criteria query
                               [:and
                                {:user/first-name "John"}
                                {:user/first-name "Jane"}])))))

; Common criteria 8: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-or-conjunction
  (testing "different fields"
    (is (= '{:find [?x]
             :where [(or-join [?a ?b]
                              [?x :user/first-name ?a]
                              [?x :user/age ?b])]
             :in [?a ?b]
             :args ["John" 25]}
           (dtl/apply-criteria query
                               [:or
                                {:user/first-name "John"}
                                {:user/age 25}]))))
  (testing "same field"
    (is (= '{:find [?x]
             :where [(or-join [?a ?b]
                              [?x :user/first-name ?a]
                              [?x :user/first-name ?b])]
             :in [?a ?b]
             :args ["John" "Jane"]}
           (dtl/apply-criteria query
                               [:or
                                {:user/first-name "John"}
                                {:user/first-name "Jane"}])))))

; Common criteria 9: complex conjunction
; [:and [:or {:user/first-name "John"} {:user/age 25}] {:user/last-name "Doe"}]
(deftest query-against-a-complex-conjunction
  (is (= '{:find [?x]
           :where [[?x :user/last-name ?c]
                   (or-join [?a ?b]
                            [?x :user/first-name ?a]
                            [?x :user/age ?b])]
           :in [?a ?b ?c]
           :args ["John" 25 "Doe"]}
         (dtl/apply-criteria query
                             [:and
                              [:or
                               {:user/first-name "John"}
                               {:user/age 25}]
                              {:user/last-name "Doe"}]))))

; Common criteria 10: between predicates
; {:transaction/date [:between "2020-01-01" "2020-02-01"]}
(deftest query-against-a-between-predicate
  (is (= '{:find [?x]
           :where [[?x :transaction/date ?date]
                   [(>= ?date ?a)]
                   [(<= ?date ?b)]]
           :in [?a ?b]
           :args ["2020-01-01" "2020-12-31"]}
         (dtl/apply-criteria query
                             {:transaction/date [:between "2020-01-01" "2020-12-31"]}))
      ":between is inclusive on both ends")
  (is (= '{:find [?x]
           :where [[?x :transaction/date ?date]
                   [(> ?date ?a)]
                   [(<= ?date ?b)]]
           :in [?a ?b]
           :args ["2020-01-01" "2020-12-31"]}
         (dtl/apply-criteria query
                             {:transaction/date [:<between "2020-01-01" "2020-12-31"]}))
      ":<between is exclusive on the lower vound and inclusive of the upper")
  (is (= '{:find [?x]
           :where [[?x :transaction/date ?date]
                   [(>= ?date ?a)]
                   [(< ?date ?b)]]
           :in [?a ?b]
           :args ["2020-01-01" "2020-12-31"]}
         (dtl/apply-criteria query
                             {:transaction/date [:between> "2020-01-01" "2020-12-31"]}))
      ":between> is inclusive on the lower bound and exclusive of the upper")
  (is (= '{:find [?x]
           :where [[?x :transaction/date ?date]
                   [(> ?date ?a)]
                   [(< ?date ?b)]]
           :in [?a ?b]
           :args ["2020-01-01" "2020-12-31"]}
         (dtl/apply-criteria query
                             {:transaction/date [:<between> "2020-01-01" "2020-12-31"]}))
      ":<between> is exclusive on both ends"))

(deftest apply-a-remapped-simple-criterion
  (is (= '{:find [?x]
           :where [[?x :xt/id ?a]]
           :in [?a]
           :args [123]}
         (dtl/apply-criteria query
                             {:id 123}
                             {:remap {:id :xt/id}}))
      "The :id can be remapped")
  (is (= '{:find [?x]
           :where [[?x :xt/id ?id]
                   [(!= ?id ?a)]]
           :in [?a]
           :args [123]}
         (dtl/apply-criteria query
                             {:id [:!= 123]}
                             {:remap {:id :xt/id}}))
      "The :id can be remapped when a predicate is present"))

(deftest apply-a-comparison-criterion
  (is (= '{:find [?x]
           :where [[?x :account/balance ?balance]
                   [(>= ?balance ?a)]]
           :in [?a]
           :args [500M]}
         (dtl/apply-criteria query
                             #:account{:balance [:>= 500M]}))))

(deftest apply-a-not-equal-criterion
  (is (= '{:find [?x]
           :where [[?x :user/name ?name]
                   [(!= ?name ?a)]]
           :in [?a]
           :args ["John"]}
         (dtl/apply-criteria query
                             #:user{:name [:!= "John"]}))))

(deftest prioritize-relationship-where-clauses
  (is (= '{:find [?x]
           :where [[?x :user/email ?a]
                   (not [?x :model/deleted true])]
           :in [?a]
           :args ["john@doe.com"]}
         (dtl/apply-criteria '{:find [?x]
                               :where [(not [?x :model/deleted true])]}
                             {:user/email "john@doe.com"}))))

(deftest apply-an-intersection-criterion
  (is (= '{:find [?x]
           :where [[?x :transaction/transaction-date ?transaction-date]
                   [(>= ?transaction-date ?a)]
                   [(< ?transaction-date ?b)]]
           :in [?a ?b]
           :args ["2020-01-01" "2020-02-01"]}
         (dtl/apply-criteria query
                             #:transaction{:transaction-date [:and
                                                              [:>= "2020-01-01"]
                                                              [:< "2020-02-01"]]}))
      "statements are added directly to the where chain"))

(deftest apply-criteria-with-a-join
  (is (= '{:find [?x]
           :where [[?x :entity/owner ?a]
                   [?commodity :commodity/entity ?x]
                   [?commodity :commodity/symbol ?b]]
           :in [?a ?b]
           :args [101 "USD"]}
         (dtl/apply-criteria query
                             {:entity/owner 101
                              :commodity/symbol "USD"}
                             {:target :entity
                              :relationships #{[:user :entity]
                                               [:entity :commodity]}
                              :graph-apex :user}))))

(deftest query-combines-redundant-and-groups
  (is (= '{:find [?x]
           :in [?a ?b ?c ?d]
           :args ["2020-01-01" "2020-01-03" 101 102]
           :where
           [[?x :transaction/date ?date]
            [(>= ?date ?a)]
            [(<= ?date ?b)]
            (or-join [?c ?d]
              [?transaction-item
               :transaction-item/debit-account
               ?c]
              [?transaction-item
               :transaction-item/credit-account
               ?d])]}
         (dtl/apply-criteria query
                             [:and
                              #:transaction{:date
                                            [:between
                                             "2020-01-01"
                                             "2020-01-03"]}
                              [:or
                               #:transaction-item{:debit-account {:id 101}}
                               #:transaction-item{:credit-account {:id 102}}]]
                             {:target :transaction}))))

(deftest apply-options
  (testing "limit"
    (is (= '{:find [?x]
             :limit 1}
           (dtl/apply-options query
                              {:limit 1}))
        "The limit attribute is copied"))
  (testing "sorting"
    (is (= '{:find [?x ?size]
             :where [[?x :shirt/size ?size]]
             :order-by [[?size :asc]]}
           (dtl/apply-options query
                              {:order-by :shirt/size}))
        "A single column is symbolized and ascended is assumed")
    (is (= '{:find [?x ?size]
             :where [[?x :shirt/size ?size]]
             :order-by [[?size :desc]]}
           (dtl/apply-options query
                              {:order-by [[:shirt/size :desc]]}))
        "An explicit direction is copied")
    (is (= '{:find [?x ?size ?weight]
             :where [[?x :shirt/size ?size]
                     [?x :shirt/weight ?weight]]
             :order-by [[?size :asc]
                        [?weight :desc]]}
           (dtl/apply-options query
                              {:order-by [:shirt/size [:shirt/weight :desc]]}))
        "Multiple fields are handled appropriately")))
