(defproject stowaway "0.2.4"
  :description "Library for abstracting data storage from business logic"
  :url "https://github.com/dgknght/stowaway"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.clojure/spec.alpha "0.5.238"]
                 [camel-snake-kebab "0.4.3"]
                 [com.github.seancorfield/honeysql "2.6.1126"]
                 [ubergraph "0.9.0" :exclusions [org.clojure/spec.alpha
                                                 org.clojure/clojure
                                                 org.clojure/core.specs.alpha]]]
  :plugins [[lein-cloverage "1.2.2"]]
  :cloverage {:line-fail-threshold 90
              :form-fail-threshold 80
              :low-watermark 93
              :high-watermark 97}
  :repl-options {:init-ns stowaway.core}
  :repositories [["clojars" {:creds :gpg}]])
