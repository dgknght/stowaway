(ns stowaway.core-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.pprint :refer [pprint]]
            [stowaway.core :as sto]))

(deftest tag-a-model
  (is (= :models-ns/user
         (-> {}
             (sto/tag :models-ns/user)
             sto/tag))))

(deftest check-a-model-tag
  (let [model (sto/tag {} :models-ns/user)]
  (is (sto/tagged? model :models-ns/user))
  (is (not (sto/tagged? model :models-ns/phone-number)))))

(deftype TestStorage [config]
  sto/Storage
  (select [_ criteria options] (pprint {:select criteria :options options}))
  (create [_ model] (pprint {:create model}))
  (update [_ model] (pprint {:update model}))
  (delete [_ model] (pprint {:delete model})))

(deftest register-a-storage-strategy
  (sto/register-strategy (fn [config]
                           (when (re-find #"\Atest" config)
                             (TestStorage. config))))
  (sto/with-storage [s "test://blah-blah"]
    (is (instance? TestStorage s))))
