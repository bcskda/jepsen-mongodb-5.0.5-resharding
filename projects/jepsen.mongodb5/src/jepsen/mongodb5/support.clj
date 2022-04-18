(ns jepsen.mongodb5.support
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.control.scp :as cscp]))

(def download-prefix "https://fastdl.mongodb.org/linux/")

(def mongodb-prefix "/opt/mongodb5")

(def mongod-binary-path (str mongodb-prefix "/bin/mongod"))

(def mongod-config-path (str mongodb-prefix "/mongod.custom.conf"))

(def mongod-logfile (str mongodb-prefix "/log/mongod.log"))

(def mongod-pidfile (str mongodb-prefix "/mongod.pid"))

(def mongosh-prefix "/opt/mongosh")

(def mongosh-binary-path (str mongosh-prefix "/bin/mongo"))

(defn tarball-url
  "URL for Debian 10 tarball"
  [version]
  (str "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian10-"
       version
       ".tgz"))

(defn prepare-dir
  "Remove-create a directory"
  [directory]
  (c/exec "rm" "-rf" directory)
  (c/exec "mkdir" "-p" directory))

(defn install-mongodb
  "Install mongod 5.x tarball"
  [version]
  (prepare-dir mongodb-prefix)
  (let [url (str download-prefix
                 "mongodb-linux-x86_64-debian10-"
                 version
                 ".tgz")]
    (cu/install-archive! url mongodb-prefix)))

(defn install-mongosh
  "Install mongosh 5.x tarball"
  [version]
  (prepare-dir mongosh-prefix)
  (let [url (str download-prefix
                 "mongodb-shell-linux-x86_64-debian10-"
                 version
                 ".tgz")]
    (cu/install-archive! url mongosh-prefix)))

(defn configure-mongod
  "Install mongod config file and create directories"
  [rs-name]
  (c/su
    (let [data-directory (str mongodb-prefix "/data")
          log-directory (str mongodb-prefix "/log")
          config-template (slurp (io/resource "mongod.conf"))
          config (format config-template rs-name)]
      (cu/write-file! config mongod-config-path)
      (prepare-dir data-directory)
      (prepare-dir log-directory))))

(defn replica-set-members-js
  "String with JS array to supply as `members` to `rs.initiate`"
  [nodes]
  (let [node-ids (range (count nodes))
        mapper (fn [id n] (format "{_id: %d, host: \"%s\"}" id n))
        nodes-js (map mapper node-ids nodes)]
    (str "["
         (clojure.string/join "," nodes-js)
         "]")))

(defn replica-set-initiate
  "Initiate replica-set (call from only one node)"
  [rs-name rs-nodes]
  (let [remote-script-path (str (cu/tmp-file!))
        script-template (slurp (io/resource "rs-init.js"))
        rs-nodes-js (replica-set-members-js rs-nodes)
        script (format script-template rs-name rs-nodes-js)]
    (cu/write-file! script remote-script-path)
    (c/exec mongosh-binary-path "localhost" remote-script-path)))

(defn db
  "Mongo for this specific version"
  [version rs-name]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing mongo" version)
      (let [config-path "resources/mongod-replicated-no-sharding.conf"]
        (c/su
          (install-mongodb version)
          (configure-mongod rs-name)
          (cu/start-daemon!
            {:logfile mongod-logfile
            :pidfile mongod-pidfile
            :chdir mongodb-prefix}
            mongod-binary-path
            :--config mongod-config-path)
          (Thread/sleep 20000)
          (when (= node "n1")
            (do
              (install-mongosh version)
              (replica-set-initiate rs-name (:nodes test))
              (Thread/sleep 20000))))))

    (teardown! [_ test node]
      (info node "tearing down mongo" version)
      (c/su
        (cu/stop-daemon! mongod-binary-path mongod-pidfile)
        (c/exec :rm :-rf mongodb-prefix)
        (c/exec :rm :-rf mongosh-prefix)))

    db/LogFiles
    (log-files [_ test node]
      [mongod-logfile])))

(defn url
  [scheme host path query-options]
  (let [k=v (fn [[k v]] (str (name k) "=" v))
        query (clojure.string/join "&" (map k=v query-options))]
    (str scheme "://"
         host
         path
         (when-not (clojure.string/blank? query) (str "?" query)))))

(defn skip-nil-values
  [mapping]
  (filter (fn [[k v]] v) mapping))

(defn rand-long [n] (long (rand n)))
