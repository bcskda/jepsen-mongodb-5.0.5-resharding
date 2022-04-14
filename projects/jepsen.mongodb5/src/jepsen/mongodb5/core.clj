(ns jepsen.mongodb5.core
  (:require [clojure.tools.logging :refer :all]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb5.client :as mongo-client]
            [jepsen.mongodb5.support :as mongo-support]
            [knossos.model :as model]
            [elle.rw-register :as elle-rw]))

(def replica-set-name "jepsen_mongodb5_simple")

(defn random-long [n] (long (rand n)))
(defn random-key [n] (str "key" (random-long n)))
(defn r   [_ _] {:type :invoke, :f :read, :key "key0", :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :key "key0", :value (random-long 5)})

(defn elle-rw-r   [_ _] {:type :invoke, :f :read, :key (random-key 10), :value nil})
(defn elle-rw-w   [_ _] {:type :invoke, :f :write, :key (random-key 10), :value (random-long 1e10)})

(defn mongodb5-test-base
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "mongo"
          :os              debian/os
          :rs-name         replica-set-name
          :db              (mongo-support/db "5.0.5" replica-set-name)
          :client          (mongo-client/client)}))

(defn ellify-op
  [op]
  (case op
    :read :r
    :write :w
    op))

(defn ellify-event
  [event]
  {:index (:index event)
   :type (:type event)
   :process (:process event)
   :value [[(ellify-op (:f event)) (:key event) (:value event)]]})

(defn ellify-history
  [jepsen-history]
  (map ellify-event jepsen-history))

(defn elle-rw-checker
  ([]
   (elle-rw-checker {}))
  ([opts]
   (reify checker/Checker
     (check [this test history checker-opts]
       (let [directory (-> (store/path! test (:subdirectory checker-opts) "elle")
                           (.getCanonicalPath))]
          (elle-rw/check (assoc opts :directory directory)
                         (ellify-history history)))))))

(defn unsafe-concerns-break-rw-reg
  [opts]
  (merge (mongodb5-test-base opts)
         {:conn-opts       {:replicaSet replica-set-name
                            :w 1
                            :readPreference "nearest"}
          :txn-opts        {:w "journaled"
                            :readConcern "local"
                            :readPreference "secondary"}
          :causally-cst    false
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

(defn unsafe-concerns-break-rw-reg--elle-rw
  [opts]
  (merge (mongodb5-test-base opts)
         {:conn-opts       {:replicaSet replica-set-name
                            :w 1
                            :readPreference "nearest"}
          :txn-opts        {:w "journaled"
                            :readConcern "local"
                            :readPreference "secondary"}
          :causally-cst    false
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker)
          :generator       (->> (gen/reserve 1 (repeat elle-rw-w)
                                             (- (:concurrency opts) 1) (repeat elle-rw-r))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 10))}))

(defn unsafe-concerns-break-rw-reg-2--elle-rw
  [opts]
  (merge (mongodb5-test-base opts)
         {:conn-opts       {:replicaSet replica-set-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "nearest"}
          :txn-opts        {:w "journaled"
                            :readConcern "majority"
                            :readPreference "nearest"}
          :causally-cst    false
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker)
          :generator       (->> (gen/reserve 1 (repeat elle-rw-w)
                                             (- (:concurrency opts) 1) (repeat elle-rw-r))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 10))}))

(defn safe-concerns-pass-rw-reg--elle-rw
  [opts]
  (merge (mongodb5-test-base opts)
         {:conn-opts       {:replicaSet replica-set-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "primary"}
          :txn-opts        {:w "journaled"
                            :readConcern "majority"
                            :readPreference "primary"}
          :causally-cst    false
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker)
          :generator       (->> (gen/reserve 1 (repeat elle-rw-w)
                                             (- (:concurrency opts) 1) (repeat elle-rw-r))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}
                                          (gen/sleep 2)
                                          {:type :info, :f :start}
                                          (gen/sleep 8)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 60))}))

(defn safe-concerns-pass-rw-reg-mixed--elle-rw
  [opts]
  (merge (mongodb5-test-base opts)
         {:conn-opts       {:replicaSet replica-set-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "primary"}
          :txn-opts        {:w "journaled"
                            :readConcern "majority"
                            :readPreference "primary"}
          :causally-cst    false
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker)
          :generator       (->> (gen/mix [(repeat elle-rw-w)
                                          (repeat elle-rw-r)])
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}
                                          (gen/sleep 2)
                                          {:type :info, :f :start}
                                          (gen/sleep 8)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 60))}))


(defn all-test-fns
  [opts]
  (map #(% opts) [unsafe-concerns-break-rw-reg
                  unsafe-concerns-break-rw-reg--elle-rw
                  unsafe-concerns-break-rw-reg-2--elle-rw
                  safe-concerns-pass-rw-reg--elle-rw
                  safe-concerns-pass-rw-reg-mixed--elle-rw]))

(defn -main
  "Handles cmdline. Can run a test or a webserver to observe results"
  [& args]
  (prn "Command line:" args)
  (cli/run! (merge (cli/single-test-cmd {:test-fn safe-concerns-pass-rw-reg-mixed--elle-rw})
                   (cli/test-all-cmd {:tests-fn all-test-fns})
                   (cli/serve-cmd))
            args))
