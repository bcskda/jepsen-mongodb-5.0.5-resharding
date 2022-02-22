(ns jepsen.mongodb5.client
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [client :as client]]
            [jepsen.mongodb5.driver :refer [write-v read-v] :as driver])
  (:import jepsen.mongodb5.driver.KvCollection)
  (:import com.mongodb.ClientSessionOptions)
  (:import com.mongodb.client.MongoClients))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (let [connString (str "mongodb://"
                          node
                          "/"
                          "?replicaSet=" (:rs-name test))
          conn (MongoClients/create connString)]
      (assoc this :conn conn)))

  (setup! [_ test])

  (invoke! [this test op]
    (let [session-opts (->
            (ClientSessionOptions/builder)
            (.causallyConsistent true)
            (.build))
          kvColl (driver/kv-collection (:conn this)
                                       "test_db"
                                       "test_collection"
                                       session-opts)]
      (case (:f op)
        :read (assoc op :type :ok :value (read-v kvColl "id0"))
        :write (do (write-v kvColl "id0" (:value op))
                   (assoc op :type :ok)))))

  (teardown! [_ test])

  (close! [this test]
    (.close (:conn this))))

(defn client []
  (Client. nil))
