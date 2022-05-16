(ns jepsen.mongodb5.tests
  (:require [elle.rw-register :as elle-rw]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb5.client :as mongo-client]
            [jepsen.mongodb5.support :refer [rand-long] :as mongo-support]))

(defn rand-key [n] (str "key" (rand-long n)))

;
; Operations: read, write, read-modify-write txn
;

(defn elle-rw-r   [_ _] {:type :invoke, :f :read, :key (rand-key 10), :value nil})

(defn elle-rw-w   [_ _] {:type :invoke, :f :write, :key (rand-key 10), :value (rand-long 1e10)})

(defn elle-txn--rmw
  [modifier]
  (fn [] 
      (let [trace (rand-long 1e6)
            op {:type :invoke,
                :f :rmw,
                :key (rand-key 4),
                :value modifier}]
        {:type :invoke, :f :txn, :value [op], :trace trace})))

(defn op-reshard
  [db-ns new-key]
  (fn []
    {:type :invoke
     :f :reshard
     :key db-ns
     :value new-key}))

;
; Checker
;

(defn ellify-op
  [op]
  (case op
    :read :r
    :write :w
    op))

(defn ellify-event-in-txn
  [event]
  (case (:f event)
         :read  (assoc event :f :r)
         :write (assoc event :f :w)
         :rmw   (let [values (case (:type event)
                               :ok (mapv #(get (:value event) %) [0 1])
                               [nil nil])]
                  [(assoc event :f :r :value (get values 0))
                   (assoc event :f :w :value (get values 1))])))

(defn ellify-txn
  [txn-events]
  (->> txn-events
       (map ellify-event-in-txn)
       (flatten)
       (map #(vector (:f %) (:key %) (:value %)))))

(defn ellify-event-top-level
  [event]
  {:index (:index event)
   :type (:type event)
   :time (:time event)
   :process (:process event)
   :value (case (:f event)
     :txn (ellify-txn (:value event))
     [[(ellify-op (:f event)) (:key event) (:value event)]])})

(defn ellify-history
  [jepsen-history]
  (map ellify-event-top-level
       (remove #(contains? #{:start :stop :reshard} (:f %)) jepsen-history)))


(defn elle-rw-checker
  ([]
   (elle-rw-checker {}))
  ([opts]
   (reify checker/Checker
     (check [this test history checker-opts]
       (let [directory (-> (store/path! test (:subdirectory checker-opts) "elle")
                           (.getCanonicalPath))
             fixed-history (ellify-history history)]
          (print (mapv #(get fixed-history %) [0 1 2 10 15 20]))
          (elle-rw/check (assoc opts :directory directory)
                          fixed-history))))))

;
; Special nemesis: partitions and resharding
;

(defn nemesis-add-reshard
  [underlying db-ns new-key]
  (reify nemesis/Nemesis
    (setup! [_ test]
      (nemesis/setup! underlying test))
    (invoke! [_ test op]
      (case (:f op)
            :reshard (let [output (mongo-support/reshard-collection db-ns new-key)]
                       (assoc op :output output))
            (nemesis/invoke! underlying test op)))
    (teardown! [_ test]
      (nemesis/teardown! underlying test))))

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
  (merge (test-base rs-name opts)
         {:conn-opts       {:replicaSet rs-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "primary"}
          :txn-opts        {:w "journaled"
                            :readConcern "majority"
                            :readPreference "primary"}
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker)
          :generator       (->> (gen/reserve 1 (repeat elle-rw-w)
                                             (- (:concurrency opts) 1) (repeat elle-rw-r))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  ; FIXME these sleep generators are not reentrant,
                                  ; they exit immediately on 2nd+ call
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit 10))}))

(defn single-document-linearizable
  [rs-name opts]
  (merge (test-base rs-name opts)
         {:conn-opts       {:replicaSet rs-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "nearest"}
          :txn-opts        {:w "majority"
                            :readConcern "majority"
                            :readPreference "nearest"}
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker {:consistency-models [:linearizable]})
          :generator       (->> (gen/mix [(repeat elle-rw-w)
                                          (repeat elle-rw-r)])
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  ; FIXME these sleep generators are not reentrant,
                                  ; they exit immediately on 2nd+ call
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
  (merge (test-base rs-name opts)
         {:conn-opts       {:replicaSet rs-name
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "primary"}
          :txn-opts        {:w "majority"
                            :readConcern "majority"
                            :readPreference "primary"}
          :nemesis         (nemesis/partition-random-halves)
          :checker         (elle-rw-checker {:consistency-models [:snapshot-isolation]})
          ; Fetch-add would write the same value multple times
          ; and cause elle/rw_register to fail
          ;:generator       (->> (repeat (elle-txn--rmw {:f :add, :value 1970}))
          :generator       (->> (repeat (elle-txn--rmw {:f :random, :value 1e9}))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  ; FIXME these sleep generators are not reentrant,
                                  ; they exit immediately on 2nd+ call
                                  (cycle [(gen/sleep 1)
                                          {:type :info, :f :start}
                                          (gen/sleep 4)
                                          {:type :info, :f :stop}
                                          (gen/sleep 2)
                                          {:type :info, :f :start}
                                          (gen/sleep 8)
                                          {:type :info, :f :stop}]))
                                (gen/time-limit (or (int (:time-limit opts)) 60)))}))

(defn try-multishard-deployment
  [rs-name opts]
  (merge (test-base rs-name opts)
         {:sharded true
          :conn-opts       {:port 55555
                            :w "majority"
                            :readConcernLevel "majority"
                            :readPreference "nearest"}
          :txn-opts        {:w "majority"
                            :readConcern "majority"
                            :readPreference "nearest"}
          :nemesis         (nemesis-add-reshard
                             (nemesis/partition-random-halves)
                             "test_db.test_collection"
                             "{_id: 1}")
          ;:nemesis         (nemesis/noop)
          :checker         (elle-rw-checker {:consistency-models [:snapshot-isolation]})
          ; Fetch-add would write the same value multple times
          ; and cause elle/rw_register to fail
          ;:generator       (->> (repeat (elle-txn--rmw {:f :add, :value 1970}))
          :generator       (->> (repeat (elle-txn--rmw {:f :random, :value 1e9}))
                                (gen/stagger 0.1)
                                (gen/nemesis
                                  (gen/phases (gen/sleep 20)
                                              ; 4x times: short + long partition, ~650 sec
                                              ;(gen/cycle 4 [{:type :info, :f :start}
                                              ;              (gen/sleep 43)
                                              ;              {:type :info, :f :stop}
                                              ;              (gen/sleep 13)
                                              ;              {:type :info, :f :start}
                                              ;              (gen/sleep 63)
                                              ;              {:type :info, :f :stop}
                                              ;              (gen/sleep 43)])
                                              ; Short partitions during resharding, ~600 sec
                                              ; TODO, no fault injection here for now
                                              (gen/once {:type :info, :f :reshard})
                                              {:type :info, :f :start}
                                              (gen/sleep 10)
                                              {:type :info, :f :stop}
                                              ))
                                              ;(gen/sleep 390)
                                              ; Long and short partitions after resharding, ~
                                              ;(gen/cycle 3 [{:type :info, :f :start}
                                              ;              (gen/sleep 63)
                                              ;              {:type :info, :f :stop}
                                              ;              (gen/sleep 33)
                                              ;              {:type :info, :f :start}
                                              ;              (gen/sleep 33)
                                              ;              {:type :info, :f :stop}
                                              ;              (gen/sleep 63)])))
                                (gen/time-limit 900))}))

(def all-tests [unsafe-concerns-not-linearizable
                single-document-linearizable
                single-shard-only-snapshot-isolation])
