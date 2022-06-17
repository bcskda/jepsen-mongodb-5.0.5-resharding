(ns jepsen.mongodb5.tests.operations
  (:require [jepsen.mongodb5.support :refer [rand-long] :as mongo-support]))

(defn rand-key [n] (str "key" (rand-long n)))

(defn elle-rw-r   [_ _] {:type :invoke, :f :read, :key (rand-key 10), :value nil})

(defn elle-rw-w   [_ _] {:type :invoke, :f :write, :key (rand-key 10), :value (rand-long 1e10)})

(defn elle-txn--rmw
  [modifier]
  (fn [] 
      (let [trace (rand-long 1e6)
            op {:type :invoke,
                :f :rmw,
                :key (rand-key 1000),
                :value modifier}]
        {:type :invoke, :f :txn, :value [op], :trace trace})))

(defn op-reshard
  [db-ns new-key]
  (fn []
    {:type :invoke
     :f :reshard
     :key db-ns
     :value new-key}))
