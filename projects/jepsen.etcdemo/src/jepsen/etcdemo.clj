(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn db
  "Etcd for this version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version))
    (teardown! [_ test node]
      (info node "tearing down etcd" version))))

(defn etcd-test
  "Options -> test map"
  [opts]
  (merge tests/noop-test
         opts
         {:name            "etcd"
          :os              debian/os
          :db              (db "v3.1.5")
          :pure-generators true}))

(defn -main
  "Handles cmdline. Can run a test or a webserver to observe results"
  [& args]
  (prn "Command line:" args)
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
                   (cli/serve-cmd))
            args))