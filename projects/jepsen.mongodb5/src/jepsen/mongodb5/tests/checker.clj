(ns jepsen.mongodb5.tests.checker
  (:require [elle.rw-register :as elle-rw]
            [jepsen [checker :as checker]
                    [store :as store]]))

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
