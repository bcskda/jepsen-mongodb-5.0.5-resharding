(ns jepsen.mongodb5.core
  (:require [jepsen.cli :as cli]
            [jepsen.mongodb5.tests :as mongo-tests]))

;
; CLI
;

(def replica-set-name "jepsen_mongodb5_simple")

(defn default-test-fn
  [opts]
  (mongo-tests/resharding-survives-primary-failover replica-set-name opts))

(defn all-test-fns
  [opts]
  (map #(% replica-set-name opts) mongo-tests/all-tests))

(defn -main
  "Handles cmdline. Can run a test or a webserver to observe results"
  [& args]
  (prn "Command line:" args)
  (cli/run! (merge (cli/single-test-cmd {:test-fn default-test-fn})
                   (cli/test-all-cmd {:tests-fn all-test-fns})
                   (cli/serve-cmd))
            args))
