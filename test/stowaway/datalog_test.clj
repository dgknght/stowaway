(ns stowaway.datalog-test
  (:require [clojure.test :refer [deftest testing is]]
            [stowaway.datalog :as dtl]))

(def ^:private query '{:find [?x]})

; Common criteria 1: single field match
; #:user{:last-name "Doe"}
(deftest apply-a-simple-criterion
  (is (= '{:find [?x]
           :where [[?x :user/last-name ?last-name-in]]
           :in [?last-name-in]
           :args ["Doe"]}
         (dtl/apply-criteria query
                             #:user{:last-name "Doe"}))))

(deftest apply-a-simple-criterion-and-specify-the-entity-var
  (is (= '{:find [?usr]
           :where [[?usr :user/first-name ?first-name-in]]
           :in [?first-name-in]
           :args ["John"]}
         (dtl/apply-criteria '{:find [?usr]}
                             #:user{:first-name "John"}
                             {:vars {:user '?usr}}))))

; Common criteria 2: model id
; {:id "101"}
(deftest apply-a-simple-id-criterion
  (testing "with implicit target entity"
    (is (= '{:find [(pull ?x [*])]
             :where [[?x :entity/name ?name]]
             :in [?x]
             :args ["101"]}
           (dtl/apply-criteria '{:find [(pull ?x [*])]
                                 :where [[?x :entity/name ?name]]}
                               {:id "101"}))))
  (testing "with explicit target entity"
    (is (= '{:find [(pull ?x [*])]
             :where [[?x :entity/name ?name]]
             :in [?x]
             :args ["101"]}
           (dtl/apply-criteria '{:find [(pull ?x [*])]
                                 :where [[?x :entity/name ?name]]}
                               {:id "101"}
                               {:target :entity})))))

; Common criteria 3: query with a predicate
; {:id [:!= "101"]}
(deftest apply-id-criterion-with-predicate
  (is (= '{:find [(pull ?x [*])]
           :where [[?x :entity/name ?name]
                   [(!= ?x ?x-in)]]
           :in [?x-in]
           :args ["101"]}
         (dtl/apply-criteria '{:find [(pull ?x [*])]
                               :where [[?x :entity/name ?name]]}
                             {:id [:!= "101"]}))))

; Common criteria 4: multiple simple equality criteria
; #:user{:first-name "John"
;        :age 25}
(deftest apply-multiple-simple-equality-criteria
  (is (= '{:find [?x]
           :where [[?x :user/first-name ?first-name-in]
                   [?x :user/age ?age-in]]
           :in [?first-name-in ?age-in]
           :args ["John" 25]}
         (dtl/apply-criteria query
                             #:user{:first-name "John"
                                    :age 25}))))

; Common criteria 5: model reference
; {:order/user {:id 101}}
(deftest query-against-criteria-with-a-model-reference
  (is (= '{:find [?x]
           :where [[?x :order/user ?user-in]]
           :in [?user-in]
           :args [101]}
         (dtl/apply-criteria query
                             {:order/user {:id 101}}))))

; Common criteria 6: subquery against attributes
; {:user/identities [:including {:identity/oauth-provider "google" :identity/oauth-id "abc123"}]}
(deftest query-against-subquery-criteria
  (testing "associated entities with attributes"
    (is (= '{:find [?x]
             :where [[?x :user/identities ?identity]
                     [?identity :identity/oauth-provider ?oauth-provider-in]
                     [?identity :identity/oauth-id ?oauth-id-in]]
             :in [?oauth-provider-in ?oauth-id-in]
             :args ["google" "abc123"]}
           (dtl/apply-criteria query
                               {:user/identities [:including
                                                  #:identity{:oauth-provider "google"
                                                             :oauth-id "abc123"}]}))))
  (testing "associated tuples"
    (is (= '{:find [?x]
             :where [[?x :user/identities ?identities-in]]
             :in [?identities-in]
             :args [["google" "abc123"]]}
           (dtl/apply-criteria query
                               {:user/identities [:including
                                                  ["google" "abc123"]]})))))

; Common criteria 7: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-and-conjunction
  (is (= '{:find [?x]
           :where [[?x :user/first-name ?first-name-in]
                   [?x :user/age ?age-in]]
           :in [?first-name-in ?age-in]
           :args ["John" 25]}
         (dtl/apply-criteria query
                             [:and
                              {:user/first-name "John"}
                              {:user/age 25}]))))

; Common criteria 8: "and" conjunction
; [:and {:user/first-name "John"} {:user/age 25}]
(deftest query-against-an-or-conjunction
  (is (= '{:find [?x]
           :where (or [?x :user/first-name ?first-name-in]
                      [?x :user/age ?age-in])
           :in [?first-name-in ?age-in]
           :args ["John" 25]}
         (dtl/apply-criteria query
                             [:or
                              {:user/first-name "John"}
                              {:user/age 25}]))))

; Common criteria 9: complex conjunction
; [:and [:or {:user/first-name "John"} {:user/age 25}] {:user/last-name "Doe"}]
(deftest query-against-a-complex-conjunction
  (is (= '{:find [?x]
           :where (and (or [?x :user/first-name ?first-name-in]
                           [?x :user/age ?age-in])
                       [?x :user/last-name ?last-name-in])
           :in [?first-name-in ?age-in ?last-name-in]
           :args ["John" 25 "Doe"]}
         (dtl/apply-criteria query
                             [:and
                              [:or
                               {:user/first-name "John"}
                               {:user/age 25}]
                              {:user/last-name "Doe"}]))))

(deftest specify-the-query-key-prefix
  (is (= {:query '{:find [?x]
                   :where [[?x :entity/name ?name-in]]
                   :in [?name-in]}
          :args ["Personal"]}
         (dtl/apply-criteria {:query query}
                             #:entity{:name "Personal"}
                             {:query-prefix [:query]}))))

(deftest apply-a-remapped-simple-criterion
  (is (= '{:find [?x]
           :where [[?x :xt/id ?id-in]]
           :in [?id-in]
           :args [123]}
         (dtl/apply-criteria query
                             {:id 123}
                             {:remap {:id :xt/id}}))))

(deftest apply-a-comparison-criterion
  (is (= '{:find [?x]
           :where [[?x :account/balance ?balance]
                   [(>= ?balance ?balance-in)]]
           :in [?balance-in]
           :args [500M]}
         (dtl/apply-criteria query
                             #:account{:balance [:>= 500M]}))))

(deftest apply-a-not-equal-criterion
  (is (= '{:find [?x]
           :where [[?x :user/name ?name]
                   [(!= ?name ?name-in)]]
           :in [?name-in]
           :args ["John"]}
         (dtl/apply-criteria query
                             #:user{:name [:!= "John"]}))))

(deftest apply-an-intersection-criterion
  (is (= '{:find [?x]
           :where [[?x :transaction/transaction-date ?transaction-date]
                   [(>= ?transaction-date ?transaction-date-in-1)]
                   [(< ?transaction-date ?transaction-date-in-2)]]
           :in [?transaction-date-in-1 ?transaction-date-in-2]
           :args ["2020-01-01" "2020-02-01"]}
         (dtl/apply-criteria query
                             #:transaction{:transaction-date [:and
                                                              [:>= "2020-01-01"]
                                                              [:< "2020-02-01"]]}))
      "statements are added directly to the where chain"))

(deftest apply-criteria-with-a-join
  (is (= '{:find [?x]
           :where [[?x :entity/owner ?owner-in]
                   [?commodity :commodity/entity ?x]
                   [?commodity :commodity/symbol ?symbol-in]]
           :in [?owner-in ?symbol-in]
           :args [101 "USD"]}
         (dtl/apply-criteria query
                             {:entity/owner 101
                              :commodity/symbol "USD"}
                             {:target :entity
                              :relationships #{[:user :entity]
                                               [:entity :commodity]}
                              :graph-apex :user}))))

(deftest apply-a-tuple-matching-criterion
  ; here it's necessary to use the := operator explicitly so that
  ; the query logic doesn't mistake :google for the operator
  (is (= '{:find [?x]
           :where [[?x :user/identities ?identities-in]]
           :in [?identities-in]
           :args [[:google "abc123"]]}
         (dtl/apply-criteria query
                             #:user{:identities [:= [:google "abc123"]]}))))

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
