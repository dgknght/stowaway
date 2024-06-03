(ns stowaway.inflection)

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

(defn plural
  [word]
  (apply-rules word ->plural-rules))

(def ^:private ->singular-rules
  [{:pattern #"(?i)\Achildren\z"
    :f (constantly "child")}
   {:pattern #"\A(.+)ies\z"
    :f #(str (second %) "y")}
   {:pattern #"\A(.+)ses\z"
    :f #(str (second %) "s")}
   {:pattern #"\A(.+)s\z"
    :f #(str (second %))}])

(defn singular
  [word]
  (apply-rules word ->singular-rules))
