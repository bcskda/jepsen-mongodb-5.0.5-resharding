(defproject jepsen.mongodb5 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "1.5.648"]
                 [elle "0.1.4"]
                 [jepsen "0.2.5"]
                 [org.mongodb/mongodb-driver-sync "4.4.0"]]
  :resource-paths ["resources"]
  :main jepsen.mongodb5.core
  :repl-options {:init-ns jepsen.mongodb5}
  :jvm-opts ["-Djava.awt.headless=true"])
