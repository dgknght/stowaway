(ns stowaway.implicit
  (:refer-clojure :exclude [update])
  (:require [stowaway.core :as core]))

(def ^:dynamic *storage* nil)

(defn unbound?
  [value]
  (instance? clojure.lang.Var$Unbound value))

(defn bound-val
  [value]
  (when-not (unbound? value)
    value))

(defmacro with-storage
  "Binds a reified storage stratey to *storage*

  (with-storage config
    (update my-model))"
  [config & body]
  `(binding [*storage* (core/reify-storage
                         (or (bound-val *storage*)
                             ~config))]
     ~@body))

(defmacro with-transacted-storage
  "Binds a transaction strategy to *storage*
  
  (with-transacted-storage config
    (update my-first-model)
    (update my-other-model))"
  [config & body]
  `(let [s# (core/reify-storage (or (bound-val *storage*)
                                    ~config))]
     (core/with-transaction s# #(binding [*storage* %]
                                  ~@body))))

(defn- assert-storage-present []
  (when (unbound? *storage*)
    (throw (RuntimeException. "No storage strategy has been specified."))))

(defn create
  [model]
  (assert-storage-present)
  (core/create *storage* model))

(defn select
  [criteria options]
  (assert-storage-present)
  (core/select *storage* criteria options))

(defn update
  ([model]
  (assert-storage-present)
   (core/update *storage* model))
  ([attr criteria]
  (assert-storage-present)
   (core/update *storage* attr criteria)))

(defn delete
  [model]
  (assert-storage-present)
  (core/delete *storage* model))

(defn delete-by
  [model]
  (assert-storage-present)
  (core/delete-by *storage* model))
