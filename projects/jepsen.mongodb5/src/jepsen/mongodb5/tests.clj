(ns jepsen.mongodb5.tests
  (:require [clojure.tools.logging :refer :all]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nemesis_combined]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb5.client :as mongo-client]
            [jepsen.mongodb5.support :refer [rand-long] :as mongo-support]
            [jepsen.mongodb5.tests.checker :refer :all]
            [jepsen.mongodb5.tests.operations :refer :all]
            [jepsen.mongodb5.reshard_nemesis :refer [blocking-reshard-nemesis]]))


;
; Tests
;

(defn test-base
  [rs-name opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "mongo"
          :os              debian/os
          :rs-name         rs-name
          :db              (mongo-support/db "5.0.5" rs-name)
          :client          (mongo-client/client)}))

(defn unsafe-concerns-not-linearizable
  [rs-name opts]
  (merge
    (test-base rs-name opts)
    {:conn-opts  {:replicaSet rs-name
                  :w "majority"
                  :readConcernLevel "majority"
                  :readPreference "primary"}
     :txn-opts   {:w "journaled"
                  :readConcern "majority"
                  :readPreference "primary"}
     :nemesis    (nemesis/partition-random-halves)
     :checker    (elle-rw-checker)
     :generator  (->> (gen/reserve
                         1 (repeat elle-rw-w)
                         (- (:concurrency opts) 1) (repeat elle-rw-r))
                      (gen/stagger 0.1)
                      (gen/nemesis
                         (cycle [(gen/sleep 1)
                                 {:type :info, :f :start}
                                 (gen/sleep 4)
                                 {:type :info, :f :stop}]))
                      (gen/time-limit 10))}))

(defn single-document-linearizable
  [rs-name opts]
  (merge
    (test-base rs-name opts)
    {:conn-opts  {:replicaSet rs-name
                  :w "majority"
                  :readConcernLevel "majority"
                  :readPreference "nearest"}
    :txn-opts   {:w "majority"
                  :readConcern "majority"
                  :readPreference "nearest"}
    :nemesis    (nemesis/partition-random-halves)
    :checker    (elle-rw-checker {:consistency-models [:linearizable]})
    :generator  (->> (gen/mix [(repeat elle-rw-w)
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
                      (gen/time-limit 10))}))

(defn single-shard-only-snapshot-isolation
  [rs-name opts]
  (merge
    (test-base rs-name opts)
    {:conn-opts {:replicaSet rs-name
                 :w "majority"
                 :readConcernLevel "majority"
                 :readPreference "primary"}
    :txn-opts   {:w "majority"
                  :readConcern "majority"
                  :readPreference "primary"}
    :nemesis    (nemesis/partition-random-halves)
    :checker    (elle-rw-checker
                  {:consistency-models [:serializable]})
    :generator  (->> (repeat (elle-txn--rmw {:f :random, :value 1e9}))
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

(defn resharding-survives-primary-failover
  [rs-name opts]
  (let [base (test-base rs-name opts)
        nemesis-gen (gen/phases
                      (gen/sleep 20)
                      ; Asynchronously start resharding
                      ; It takes ~300 sec to complete
                      {:type :info
                       :f :reshard-start
                       :value {:db-ns "test_db.test_collection"
                               :new-key "{_id: 1}"}}
                      ; Wait until resharding actually starts
                      (gen/sleep 20)
                      ; Partition primaries of both replica sets
                      ; to cause failover
                      {:type :info, :f :start-partition, :value :primaries}
                      (gen/sleep 10)
                      {:type :info, :f :stop-partition}
                      (gen/sleep 360)
                      {:type :info, :f :reshard-progress})]
    (merge
        base
        {:sharded true
         :conn-opts {:port 55555
                     :w "majority"
                     :readConcernLevel "majority"
                     :readPreference "nearest"}
         :txn-opts  {:w "majority"
                     :readConcern "majority"
                     :readPreference "nearest"}
         :nemesis   (nemesis/compose
                      {#{:start-partition :stop-partition}
                      (nemesis_combined/partition-nemesis (:db base))
                      ,
                      #{:reshard-start :reshard-progress}
                      (blocking-reshard-nemesis)})
         :checker   (elle-rw-checker
                      {:consistency-models [:snapshot-isolation]})
         :generator (->>
                      (repeat (elle-txn--rmw {:f :random, :value 1e9}))
                      (gen/stagger 0.1)
                      (gen/nemesis nemesis-gen)
                      (gen/time-limit 600))})))

(defn resharding-and-long-partition
  [rs-name opts]
  (let [base (test-base rs-name opts)
        nemesis-gen (gen/phases
                      (gen/sleep 20)
                      {:type :info
                       :f :reshard-start
                       :value {:db-ns "test_db.test_collection"
                               :new-key "{_id: 1}"}}
                      (gen/sleep 20)
                      (let [primaries #{"n1" "n4" "n7"}
                            non-primaries (vec (remove
                                                 #(contains? primaries %)
                                                 (:nodes base)))
                            partition-spec [["n1"] ["n4"] ["n7"] non-primaries]]
                          {:type :info,
                           :f :start-partition,
                           :value (nemesis/complete-grudge partition-spec)})
                      (gen/sleep 540)
                      {:type :info, :f :stop-partition}
                      (gen/sleep 10)
                      {:type :info, :f :reshard-progress})]
    (merge
      base
      {:sharded true
      :conn-opts       {:port 55555
                      :w "majority"
                      :readConcernLevel "majority"
                      :readPreference "secondary"}
      :txn-opts        {:w "majority"
                      :readConcern "majority"
                      :readPreference "secondary"}
      :nemesis         (nemesis/compose {#{:start-partition :stop-partition}
                                         (nemesis_combined/partition-nemesis (:db base))
                                         ,
                                         #{:reshard-start :reshard-progress}
                                         (blocking-reshard-nemesis)})
      :checker         (elle-rw-checker {:consistency-models [:snapshot-isolation]})
      :generator       (->> (repeat (elle-txn--rmw {:f :random, :value 1e11}))
                          (gen/nemesis nemesis-gen)
                          (gen/time-limit 600))})))


(def all-tests [unsafe-concerns-not-linearizable
                single-document-linearizable
                single-shard-only-snapshot-isolation
                resharding-survives-primary-failover
                resharding-and-long-partition])
