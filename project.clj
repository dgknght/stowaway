(defproject stowaway "0.1.16"
  :description "Library for abstracting data storage from business logic"
  :url "https://github.com/dgknght/stowaway"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "1.0.0"]
                 [camel-snake-kebab "0.4.1"]
                 [com.github.seancorfield/honeysql "2.0.0-rc3"]]
  :repl-options {:init-ns stowaway.core})
