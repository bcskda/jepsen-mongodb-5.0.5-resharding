(ns jepsen.mongodb5.reshard_nemesis
  (:require [clojure.core.async :as async]
            [jepsen [nemesis :as nemesis]]
            [jepsen.mongodb5.support :refer [reshard-collection]]))

(defrecord BlockingReshardNemesis [channel]
  nemesis/Nemesis
  (setup! [this test] this)

  (invoke! [this test op]
    (case (:f op)
      :reshard-start (let [db-ns (:db-ns (:value op))
                           new-key (:new-key (:value op))
                           thread-ch (async/thread
                                       (try
                                         (let [result (reshard-collection db-ns new-key)]
                                           (println "BlockingReshardNemesis" "result:" result)
                                           (println "BlockingReshardNemesis" "offer result:" (async/offer! channel result)))
                                         (catch Throwable e (println "BlockingReshardNemesis" "exception:" e))))]
                       (assoc op :value :started))
      :reshard-progress (let [polled (async/poll! channel)]
                          (if (nil? polled)
                            (assoc op :value :not-finished)
                            (assoc op :value :finished, :output polled)))))
 
  (teardown! [this test]
    (async/close! channel)))

(defn blocking-reshard-nemesis []
  (BlockingReshardNemesis. (async/chan 1)))
