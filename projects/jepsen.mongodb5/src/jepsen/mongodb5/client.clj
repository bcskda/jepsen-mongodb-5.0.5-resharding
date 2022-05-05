(ns jepsen.mongodb5.client
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [client :as client]]
            [jepsen.mongodb5.driver :refer [write-v read-v with-txn] :as driver]
            [jepsen.mongodb5.support :refer [rand-long skip-nil-values url] :as support]
            [slingshot.slingshot :refer [try+]])
  (:import jepsen.mongodb5.driver.KvCollection)
  (:import com.mongodb.ClientSessionOptions)
  (:import com.mongodb.MongoSocketReadTimeoutException)
  (:import com.mongodb.MongoWriteConcernException)
  (:import com.mongodb.ReadConcern)
  (:import com.mongodb.ReadConcernLevel)
  (:import com.mongodb.ReadPreference)
  (:import com.mongodb.TransactionOptions)
  (:import com.mongodb.WriteConcern)
  (:import com.mongodb.client.MongoClients))

(defn txn-options
  ([w-concern r-concern r-preference]
   (->
     (TransactionOptions/builder)
     (.writeConcern (WriteConcern/valueOf w-concern))
     (.readConcern (ReadConcern. (ReadConcernLevel/fromString r-concern)))
     (.readPreference (ReadPreference/valueOf r-preference))
     (.build)))
  ([options-map]
   (txn-options (:w options-map)
                (:readConcern options-map)
                (:readPreference options-map))))

(defn session-options [txn-opts]
  (->
    (ClientSessionOptions/builder)
    (.defaultTransactionOptions txn-opts)
    (.build)))

(defn connection-string
  ([host options]
   (url "mongodb" host (:port options) "/" (skip-nil-values options)))
  ([host rs-name w read-preference]
   (let [options {:replicaSet rs-name
                 :w w
                 :readPreference read-preference}]
     (connection-string host options))))

(defn apply-rmw
  [mod-op op value]
  (if (nil? value) (rand-long 1e9)
                   (case (:f mod-op)
                     :add (+ value (:value mod-op))
                     :random (rand-long (:value mod-op)))))

(defn client-invoke
  [kv-coll op]
  (case (:f op)
    :read  (assoc op :type :ok :value (read-v kv-coll (:key op)))
    :write (do (write-v kv-coll (:key op) (:value op))
               (assoc op :type :ok))
    :rmw  (let [prev-value (read-v kv-coll (:key op))
                next-value (apply-rmw (:value op) op prev-value)
                result-r {:op :read, :type :ok, :key (:key op), :value prev-value}
                result-w {:op :write, :type :ok, :key (:key op), :value next-value}]
            (write-v kv-coll (:key op) next-value)
            (assoc op :type :ok :value [prev-value next-value]))
    :txn  (do (with-txn kv-coll (fn []
                (let [txn-ops (:value op)
                      results (map #(client-invoke kv-coll %) txn-ops)]
                  (assoc op :type :ok :value results)))))
    :reshard (let [db-ns (:key op)
                   new-key (:value op)
                   stdout (support/reshard-collection db-ns new-key)]
               (assoc op :type :ok
                         :value stdout))))
    ;:txn   (do (txn-start kv-coll)
    ;           (let [txn-ops (:value op)
    ;                    results (map #(client-invoke kv-coll %) txn-ops)]
    ;                (txn-commit kv-coll)
    ;                (assoc op :type :ok :value results)))))

(defrecord Client [conn kvColl]
  client/Client
  (open! [this test node]
    (let [connString (connection-string node (:conn-opts test))
          conn (MongoClients/create connString)
          txn-opts (txn-options (:txn-opts test))
          session-opts (session-options txn-opts)
          kvColl (driver/kv-collection conn
                                       "test_db"
                                       "test_collection"
                                       session-opts)]
      (assoc this :conn conn :kvColl kvColl)))

  (setup! [_ test])

  (invoke! [this test op]
    (client-invoke (:kvColl this) op))

  (teardown! [_ test])

  (close! [this test]
    (.close (:conn this))))

(defn client []
  (Client. nil nil))
