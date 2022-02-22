(ns jepsen.mongodb5.core
  (:require [clojure.tools.logging :refer :all]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb5.client :as mongo-client]
            [jepsen.mongodb5.support :as mongo-support]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn mongodb5-test
  "Options -> test map"
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "mongo"
          :os              debian/os
          :rs-name         "jepsen_mongodb5_simple"
          :db              (mongo-support/db "5.0.5" "jepsen_mongodb5_simple")
          :client          (mongo-client/client)
          :checker         (checker/linearizable
                             {:model (model/register)
                              :algorithm :linear})
          :generator       (->> (gen/mix [r w])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))

(defn -main
  "Handles cmdline. Can run a test or a webserver to observe results"
  [& args]
  (prn "Command line:" args)
  (cli/run! (merge (cli/single-test-cmd {:test-fn mongodb5-test})
                   (cli/serve-cmd))
            args))
