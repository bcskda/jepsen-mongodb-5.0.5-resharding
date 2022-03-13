(ns jepsen.mongodb5.client
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [client :as client]]
            [jepsen.mongodb5.driver :refer [write-v read-v] :as driver])
  (:import jepsen.mongodb5.driver.KvCollection)
  (:import com.mongodb.ClientSessionOptions)
  (:import com.mongodb.TransactionOptions)
  (:import com.mongodb.ReadConcern)
  (:import com.mongodb.ReadPreference)
  (:import com.mongodb.WriteConcern)
  (:import com.mongodb.client.MongoClients))

(defrecord Client [conn kvColl]
  client/Client
  (open! [this test node]
    (let [connString (str "mongodb://"
                          node
                          "/"
                          "?replicaSet=" (:rs-name test))
          conn (MongoClients/create connString)
          txn-opts (->
            (TransactionOptions/builder)
            (.readConcern ReadConcern/LOCAL)
            (.readPreference (ReadPreference/secondary))
            (.writeConcern WriteConcern/JOURNALED)
            (.build))
          session-opts (->
            (ClientSessionOptions/builder)
            (.causallyConsistent false)
            (.defaultTransactionOptions txn-opts)
            (.build))
          kvColl (driver/kv-collection conn
                                       "test_db"
                                       "test_collection"
                                       session-opts)]
      (assoc this :conn conn :kvColl kvColl)))

  (setup! [_ test])

  (invoke! [this test op]
    (case (:f op)
      :read (assoc op :type :ok :value (read-v (:kvColl this) "id0"))
      :write (do (write-v (:kvColl this) "id0" (:value op))
                 (assoc op :type :ok))))

  (teardown! [_ test])

  (close! [this test]
    (.close (:conn this))))

(defn client []
  (Client. nil nil))
