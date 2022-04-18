(ns jepsen.mongodb5.driver
  (:import org.bson.BsonDocument)
  (:import org.bson.BsonInt64)
  (:import com.mongodb.client.TransactionBody)
  (:import com.mongodb.client.model.Filters)
  (:import com.mongodb.client.model.UpdateOptions))

(defn long-to-bson [x] (BsonInt64. x))

(defn kvcoll-find [coll session k]
  (-> coll (.find session (Filters/eq k))
           (.first)))

(defn kvcoll-upsert [coll session k v]
    (let [updateOptions (-> (UpdateOptions.)
                            (.upsert true))
          bson (BsonDocument. "$set"
                              (BsonDocument. "value"
                                             (long-to-bson v)))]
      (-> coll (.updateOne session
                           (Filters/eq k)
                           bson
                           updateOptions))))

(defprotocol Key-Value
  (read-v [this k])
  (write-v [this k v])
  ;(txn-start [this])
  ;(txn-commit [this])
  ;(has-txn [this])
  (with-txn [this body]))

(defrecord KvCollection [coll session]
  Key-Value
  (read-v [this k]
    (let [result (kvcoll-find coll session k)]
      (when (some? result)
            (.getLong result "value"))))

  (write-v [this k v]
    (kvcoll-upsert coll session k v))

  ;(txn-start [this]
  ;  (-> session
  ;       (.startTransaction)))
  ;   ;(throw (let [options (-> session
  ;   ;                         (.getTransactionOptions))
  ;   ;             readConcern (-> options
  ;   ;                             (.getReadConcern)
  ;   ;                             (.asDocument))]
  ;   ;         (Exception. (str options readConcern))))))

  ;(txn-commit [this]
  ;  (-> session
  ;      (.commitTransaction)))

  ;(has-txn [this]
  ;  (-> session
  ;      (.hasActiveTransaction)))

  (with-txn [this func]
    (let [body (reify TransactionBody
                  (execute [_] (func)))]
      (-> session
          (.withTransaction body)))))

(defn kv-collection [conn db-name coll-name session-opts]
  (let [coll (-> conn (.getDatabase db-name)
                      (.getCollection coll-name))
        session (-> conn (.startSession session-opts))]
    (KvCollection. coll session)))
