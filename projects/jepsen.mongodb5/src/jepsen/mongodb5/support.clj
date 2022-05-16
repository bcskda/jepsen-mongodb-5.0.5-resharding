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


(def mongos-binary-path (str mongodb-prefix "/bin/mongos"))

(def mongos-config-path (str mongodb-prefix "/mongos.custom.conf"))

(def mongos-logfile (str mongodb-prefix "/log/mongos.log"))

(def mongos-pidfile (str mongodb-prefix "/mongos.pid"))


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

(defn configure-mongod-config
  [rs-name]
  (c/su
    (let [data-directory (str mongodb-prefix "/data")
          log-directory (str mongodb-prefix "/log")
          config-template (slurp (io/resource "mongod-non-sharded.conf"))
          config (format config-template rs-name)]
      (cu/write-file! config mongod-config-path)
      (prepare-dir data-directory)
      (prepare-dir log-directory))))

(defn configure-mongod-shard
  [rs-name]
  (c/su
    (let [data-directory (str mongodb-prefix "/data")
          log-directory (str mongodb-prefix "/log")
          config-template (slurp (io/resource "mongod-sharded.conf"))
          config (format config-template rs-name)]
      (cu/write-file! config mongod-config-path)
      (prepare-dir data-directory)
      (prepare-dir log-directory))))

(defn configure-mongod
  "Install mongod config file and create directories"
   ([rs-name is-shard]
    (if is-shard
        (configure-mongod-shard rs-name)
        (configure-mongod-config rs-name)))
   ([rs-name]
    (configure-mongod rs-name false)))

(defn configure-mongos
  "Install mongos config file and create directories"
   [configsvr-rs configsvr-nodes]
   (c/su
     (let [nodes (clojure.string/join "," (map #(str % ":27017") configsvr-nodes))
           config-template (slurp (io/resource "mongos.conf"))
           config (format config-template configsvr-rs nodes)]
       (cu/write-file! config mongos-config-path))))


(defn replica-set-members-js
  "String with JS array to supply as `members` to `rs.initiate`"
  [nodes]
  (let [node-ids (map #(Integer. (subs % 1)) nodes)
        mapper (fn [id n] (format "{_id: %d, host: \"%s\"}" id n))
        nodes-js (map mapper node-ids nodes)]
    (str "["
         (clojure.string/join "," nodes-js)
         "]")))

(defn replica-set-initiate-sharded
  [rs-name rs-nodes is-shard]
  (let [remote-script-path (str (cu/tmp-file!))
        script-template (slurp (io/resource "rs-init-sharded.js"))
        rs-nodes-js (replica-set-members-js rs-nodes)
        is-configsvr (not is-shard)
        script (format script-template rs-name rs-nodes-js is-configsvr)]
    (info "Initiating sharded-cluster rs" rs-name "on nodes" rs-nodes "role" (if is-shard "shard" "conf"))
    (cu/write-file! script remote-script-path)
    (c/exec mongosh-binary-path "localhost" remote-script-path)))

(defn replica-set-initiate-non-sharded
  "Initiate replica-set (call from only one node)"
  [rs-name rs-nodes]
  (let [remote-script-path (str (cu/tmp-file!))
        script-template (slurp (io/resource "rs-init-non-sharded.js"))
        rs-nodes-js (replica-set-members-js rs-nodes)
        script (format script-template rs-name rs-nodes-js)]
    (info "Initiating unsharded-cluster rs" rs-name "on nodes" rs-nodes)
    (cu/write-file! script remote-script-path)
    (c/exec mongosh-binary-path "localhost" remote-script-path)))

(defn replica-set-initiate
  "Initiate replica-set (call from only one node)"
  ([rs-name rs-nodes]
   (replica-set-initiate-non-sharded rs-name rs-nodes))
  ([rs-name rs-nodes is-shard]
   (replica-set-initiate-sharded rs-name rs-nodes is-shard)))


(defn add-shard
  [nodes rs-name]
  (let [nodes-formatted (clojure.string/join "," (map #(str % ":27017") nodes))
        remote-script-path (str (cu/tmp-file!))
        script-template (slurp (io/resource "add-shard.js"))
        script (format script-template rs-name nodes-formatted)]
    (cu/write-file! script remote-script-path)
    (c/exec mongosh-binary-path "localhost:55555" remote-script-path)))

(defn add-shard-from-node
  [shard-number shard-rs-name]
  (let [shard-nodes (map #(str "n" (+ % (* 3 shard-number))) [1 2 3])]
    (add-shard shard-nodes shard-rs-name)))


(defn shard-collection
  [db-name coll-name key-name shard-type]
  (let [remote-script-path (str (cu/tmp-file!))
        script-template (slurp (io/resource "shard-collection.js"))
        script (format script-template
                       db-name
                       db-name coll-name key-name shard-type)]
    (cu/write-file! script remote-script-path)
    (c/exec mongosh-binary-path "localhost:55555" remote-script-path)))

(defn reshard-collection
  [db-ns new-key]
  (c/with-session "n1" (c/session "n1")
    (let [remote-script-path (cu/tmp-file!)
          script-template (slurp (io/resource "reshard-collection.js"))
          script (format script-template
                         db-ns new-key)]
      (cu/write-file! script remote-script-path)
      (c/exec mongosh-binary-path "localhost:55555" remote-script-path))))


(defn local-set-of-primaries
  [node]
  (c/with-session node (c/session node)
    (let [remote-script-path (cu/tmp-file!)
          script (slurp (io/resource "local-view-of-primaries.js"))]
      (cu/write-file! script remote-script-path)
      (let [exec-stdout (c/exec mongosh-binary-path "localhost:27017" remote-script-path)]
        (apply hash-set (clojure.string/split exec-stdout #" "))))))


(defn db
  "Mongo for this specific version"
  [version rs-name]
  (reify
    db/DB
    (setup! [_ test node]
      (info "Installing mongo" version)
      (let [config-path "resources/mongod-replicated-no-sharding.conf"
            node-number (Integer. (subs node 1))
            shard-number (quot (- node-number 1) 3)
            rs-nodes (if (:sharded test)
                         (map #(str "n" (+ % (* 3 shard-number))) [1 2 3])
                         (:nodes test))
            is-rs-initiator (if (:sharded test)
                                (= (mod node-number 3) 1)
                                (= node-number 1))
            is-shard (and (:sharded test) (> shard-number 0))
            confrs-name (str rs-name "-conf")
            shardrs-name (str rs-name "-shard" shard-number)
            orig-rs-name rs-name
            rs-name (if (:sharded test)
                        (if is-shard shardrs-name confrs-name)
                        rs-name)
            mongod-common-opts [{:logfile mongod-logfile
                                 :pidfile mongod-pidfile
                                 :chdir mongodb-prefix}
                                mongod-binary-path
                                :--config mongod-config-path]
            mongod-opts (if (:sharded test)
                            (merge mongod-common-opts
                                   (if (= shard-number 0) :--configsvr :--shardsvr))
                            mongod-common-opts)]
        (c/su
          (install-mongodb version)
          (configure-mongod rs-name)
          (apply cu/start-daemon! mongod-opts)
          (Thread/sleep 20000)
          (when (:sharded test)
            (do
              ;(install-mongos version)
              (configure-mongos confrs-name ["n1" "n2" "n3"])
              (cu/start-daemon!
                {:logfile mongos-logfile
                 :pidfile mongos-pidfile
                 :chdir mongodb-prefix}
                 mongos-binary-path
                 :--config mongos-config-path)
                 )
              (Thread/sleep 20000))
          (when is-rs-initiator
            (do
              (install-mongosh version)
              (if (:sharded test)
                  (replica-set-initiate rs-name rs-nodes is-shard)
                  (replica-set-initiate rs-name rs-nodes))
              (Thread/sleep 20000)))
          (when (:sharded test) 
            (when (and (> shard-number 0) is-rs-initiator)
              (info "Adding shard" rs-name)
              (add-shard-from-node shard-number rs-name))
            (Thread/sleep 20000)
            (when (and (= shard-number 0) is-rs-initiator)
              (shard-collection "test_db" "test_collection" "_id" "\"hashed\""))
            (Thread/sleep 10000)))))

    (teardown! [_ test node]
      (info "Tearing down mongo" version)
      (c/su
        (when (:sharded test)
          (cu/stop-daemon! mongos-binary-path mongos-pidfile))
        (cu/stop-daemon! mongod-binary-path mongod-pidfile)
        (c/exec :rm :-rf mongodb-prefix)
        (c/exec :rm :-rf mongosh-prefix)))

    db/LogFiles
    (log-files [_ test node]
      [mongod-logfile mongos-logfile])

    db/Primary
    (primaries [_ test]
      (let [per-node-views (map #(local-set-of-primaries %) (:nodes test))]
       (apply clojure.set/union per-node-views)))

    (setup-primary! [_ test node]
      (info "setup-primary!" node))))

(defn url
  [scheme host port path query-options]
  (let [k=v (fn [[k v]] (str (name k) "=" v))
        query (clojure.string/join "&" (map k=v query-options))]
    (str scheme "://"
         host
         ":"
         port
         path
         (when-not (clojure.string/blank? query) (str "?" query)))))

(defn skip-nil-values
  [mapping]
  (filter (fn [[k v]] v) mapping))

(defn rand-long [n] (long (rand n)))
