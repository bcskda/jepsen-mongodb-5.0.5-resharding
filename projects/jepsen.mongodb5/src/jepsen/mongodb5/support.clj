(ns jepsen.mongodb5.support
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.control.scp :as cscp]))

(def download-prefix "https://fastdl.mongodb.org/linux/")

(def mongodb-prefix "/opt/mongodb5")

(def mongod-binary (str mongodb-prefix "/bin/mongod"))

(def mongod-config (str mongodb-prefix "/mongod.custom.conf"))

(def mongod-logfile (str mongodb-prefix "/log/mongod.log"))

(def mongod-pidfile (str mongodb-prefix "/mongod.pid"))

(def mongosh-prefix "/opt/mongosh")

(def mongosh-binary (str mongosh-prefix "/bin/mongo"))

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
  [local-config-path]
  (c/su
    (let [data-directory (str mongodb-prefix "/data")
          log-directory (str mongodb-prefix "/log")]
      (c/upload local-config-path mongod-config)
      (prepare-dir data-directory)
      (prepare-dir log-directory))))

(defn replica-set-initiate
  "Initiate replica-set (call from only one node)"
  [rs-name]
  (let [remote-script-path (str (cscp/tmp-file) ".js")
        local-script-path (str "resources/rs-init--" rs-name".js")]
    (c/upload local-script-path remote-script-path)
    (c/exec mongosh-binary "localhost" remote-script-path)))

(defn db
  "Mongo for this specific version"
  [version rs-name]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing mongo" version)
      (let [config-path "resources/mongod-replicated-no-sharding.conf"]
        (c/su
          (install-mongodb version)
          (configure-mongod config-path)
          (cu/start-daemon!
            {:logfile mongod-logfile
            :pidfile mongod-pidfile
            :chdir mongodb-prefix}
            mongod-binary
            :--config mongod-config)
          (Thread/sleep 20000)
          (when (= node "n1")
            (do
              (install-mongosh version)
              (replica-set-initiate rs-name)
              (Thread/sleep 20000))))))

    (teardown! [_ test node]
      (info node "tearing down mongo" version)
      (c/su
        (cu/stop-daemon! mongod-binary mongod-pidfile)
        (c/exec :rm :-rf mongodb-prefix)
        (c/exec :rm :-rf mongosh-prefix)))

    db/LogFiles
    (log-files [_ test node]
      [mongod-logfile])))
