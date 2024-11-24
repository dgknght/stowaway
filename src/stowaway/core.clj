(ns stowaway.core
  (:refer-clojure :exclude [update])
  (:require [clojure.tools.logging :as log]))

(derive clojure.lang.PersistentVector   ::vector)
(derive clojure.lang.PersistentHashMap  ::map)
(derive clojure.lang.PersistentArrayMap ::map)
(derive clojure.lang.MapEntry           ::map-entry)
(derive clojure.lang.Keyword            ::keyword)

(defprotocol Storage
  "This protocol defines the abstract CRUD operations to be used
  by the application. This abstraction can be implemented by whatever
  specific storage you want to use."
  (create
    [this model]
    "Creates a new model record")
  (select
    [this criteria options]
    "Selects model records matching the specified criteria")
  (update
    [this model]
    [this attr criteria]
    "Given two args, updates the specified model. Given three arguments,
    applies the specified attributes to records matching the specified
    criteria.")
  (delete
    [this model]
    "Deletes the specified model record")
  (delete-by
    [this criteria]
    "Delete all models matching the specified criteria")

  ; Data integrity utilities
  ; ------------------------

  (with-transaction
    [this func]
    "Executes the specified function which expects a single argument,
    which is a transacted storage instance"))

(def ^:private storage-handlers (atom []))

(defn register-strategy
  "Registers a data storage stragey so that it can be
  used in an application.

  The single argument is a function that will be
  called and given the configuration for the storage
  strategy. If the strategy is able to initialize itself
  with the given configuration, it returns the reified
  strategy. Otherwise it returns nil."
  [f]
  (swap! storage-handlers conj f))

; If the given storage configuration is a reified storage strategy, return it
(register-strategy #(when (satisfies? Storage %) %))

(defn reify-storage
  "Searches registered strategies for one that is able to initialize itself
  with the given confiration.

  If the given configuration implements Storage, it is returned. This
  allows a method that expects a storage configuration to receive a
  reified storage strategy also."
  [config]
  (or (some #(% config) @storage-handlers)
      (throw (RuntimeException. "Unrecognized storage specification."))))

(defmacro with-storage
  "Binds a reified storage strategy to the
  specified var.

  (with-storage [stg config]
    (storage/update stg my-model))"
  [binding & body]
  `(let [s# (reify-storage ~(second binding))
         f# (fn* [~(first binding)] ~@body)]
     (f# s#)))

(defmacro with-transacted-storage
  "Evaluates the body in the context of a transaction using the configured
  storage mechanism.
  (transacted-storage [trns config]
    (storage/update trns my-first-model)
    (storage/update trns my-other-model)))"
  [binding & body]
  `(let [s# (reify-storage ~(second binding))
         f# (fn* [~(first binding)] ~@body)]
     (with-transaction s# f#)))

(defn- ensure-tag
  [meta-data model-type]
  (when-let [existing (get-in meta-data [::type])]
    (when-not (= existing model-type)
      (log/warnf "Overwriting model tag %s with %s" existing model-type)))
  (assoc meta-data ::type model-type))

(defn tag
  "Given two arguments returns the first argument (the model) with
  updated metadata that can be used to determine how the model
  should be stored.

  Given one argument, returns the type identifier from the metadata.

  If the model already has a tag, and that tag is different that the
  second argument, a warning is written to the log."
  ([model]
   (-> model
       meta
       ::type))
  ([model model-type]
   (vary-meta model ensure-tag model-type)))

(defn tagged?
  "Returns true if the specified model is tagged
  with the specified value."
  [model model-type]
  (= model-type (tag model)))
