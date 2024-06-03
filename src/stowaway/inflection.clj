(ns stowaway.inflection
  (:require [clojure.pprint :refer [pprint]]))

(derive java.lang.String ::string)
(derive clojure.lang.Keyword ::keyword)

(defn- apply-word-rule
  [word {:keys [pattern f]}]
  (when-let [match (re-find pattern word)]
    (f match)))

(defn- apply-rules
  [word rules]
  (some (partial apply-word-rule word)
        rules))

(def ^:private ->plural-rules
  [{:pattern #"(?i)\Achild\z"
    :f #(str % "ren")}
   {:pattern #"(?i)(.+)s\z"
    :f #(str (second %) "ses")}
   {:pattern #"(?i)(.+)y\z"
    :f #(str (second %) "ies")}
   {:pattern #".+"
    :f #(str % "s")}])

(defmulti plural type)

(defmethod plural ::string
  [word]
  (apply-rules word ->plural-rules))

(defmethod plural ::keyword
  [word]
  (-> word name plural keyword))

(def ^:private ->singular-rules
  [{:pattern #"(?i)\Achildren\z"
    :f (constantly "child")}
   {:pattern #"\A(.+)ies\z"
    :f #(str (second %) "y")}
   {:pattern #"\A(.+)ses\z"
    :f #(str (second %) "s")}
   {:pattern #"\A(.+)s\z"
    :f #(str (second %))}])

(defmulti singular type)

(defmethod singular ::string
  [word]
  (when word
    (apply-rules word ->singular-rules)))

(defmethod singular ::keyword
  [word]
  (-> word name singular keyword))
