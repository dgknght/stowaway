(defproject stowaway "0.1.28"
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
  :repl-options {:init-ns stowaway.core}
  :repositories [["clojars" {:creds :gpg}]])
