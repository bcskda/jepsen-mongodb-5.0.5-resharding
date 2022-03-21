(ns jepsen.mongodb5.core
  (:require [clojure.tools.logging :refer :all]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb5.client :as mongo-client]
            [jepsen.mongodb5.support :as mongo-support]
            [knossos.model :as model]))

(def replica-set-name "jepsen_mongodb5_simple")

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn mongodb5-rw-simple-test
  [opts]
  (print opts)
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "mongo"
          :os              debian/os
          :rs-name         replica-set-name
          :db              (mongo-support/db "5.0.5" replica-set-name)
          :client          (mongo-client/client)
          :nemesis         (nemesis/partition-random-halves)
          :checker         (checker/linearizable
                             {:model (model/register)
                              :algorithm :linear})
          :generator       (->> (gen/reserve 1 (repeat w)
                                             (- (:concurrency opts) 1) (repeat r))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 10))}))

(defn -main
  "Handles cmdline. Can run a test or a webserver to observe results"
  [& args]
  (prn "Command line:" args)
  (cli/run! (merge (cli/single-test-cmd {:test-fn mongodb5-rw-simple-test})
                   (cli/serve-cmd))
            args))
