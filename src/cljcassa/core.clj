(ns cljcassa.core
  (:use cljcassa.columnfamily)
  (import java.nio.ByteBuffer)
  (import org.apache.thrift.transport.TSocket)
  (import org.apache.thrift.transport.TFramedTransport)
  (import org.apache.thrift.protocol.TBinaryProtocol)
  (import org.apache.cassandra.thrift.Cassandra$Client)
  (import org.apache.cassandra.thrift.Column)
  (import org.apache.cassandra.thrift.ColumnParent)
  (import org.apache.cassandra.thrift.ColumnPath)
  (import org.apache.cassandra.thrift.ConsistencyLevel)
  (gen-class))

(def ^:dynamic *connection*)
(def ^:dynamic *columnfamily*)

(defn current-connection
  []
  (if (bound? (var *connection*))
    *connection*
    (throw (new Exception "Not connected to a cassandra cluster!"))))

(defn current-columnfamily
  []
  (if (bound? (var *columnfamily*))
    *columnfamily*
    (throw (new Exception "You must specify a column family or wrap this call
                          in 'with-columnfamily!'"))))

(defn- get-connection
  [host port]
  (let [transport (new TFramedTransport (new TSocket host port))
        protocol (new TBinaryProtocol transport)]
    (.open transport)
    (proxy [Cassandra$Client java.io.Closeable] [protocol]
      (close [] (.close transport)))))

(defmacro with-connection
  [host port & body]
  `(with-open [conn# (get-connection ~host ~port)]
     (binding [*connection* conn#]
       ~@body)))

(defmacro with-columnfamily
  [keyspace family & body]
  `(let [conn# (current-connection)
         cf_defs# (.cf_defs (.describe_keyspace conn# ~keyspace))
         columnfamily# (create-cf ~family cf_defs#)]
     (.set_keyspace conn# ~keyspace)
     (binding [*columnfamily* columnfamily#]
       ~@body)))

(defn cluster-name [] (.describe_cluster_name (current-connection)))
(defn partitioner [] (.describe_partitioner (current-connection)))
(defn snitch [] (.describe_snitch (current-connection)))

(defn get [{:keys [key name] :as col}]
  (let [connection (current-connection)
        columnfamily (current-columnfamily)
        path (new ColumnPath (:name columnfamily))
        key (ser-key key columnfamily)
        name (ser-col-name col columnfamily)]
    (.setColumn path name)
    (deserialize-column columnfamily (.get connection key path (ConsistencyLevel/findByValue 1)))))

(defn insert [{:keys [key name value] :as col}]
  (let [connection (current-connection)
        columnfamily (current-columnfamily)
        parent (new ColumnParent (:name columnfamily))
        column (new Column)
        key (ser-key key columnfamily)
        name (ser-col-name col columnfamily)
        value (ser-col-value col columnfamily)]
    (.setName column name)
    (.setValue column value)
    (.setTimestamp column (* 1000 (System/currentTimeMillis)))
    (.insert connection key parent column (ConsistencyLevel/findByValue 1))))

(defn -main [& args]
  (with-connection "localhost" 9160
    (with-columnfamily "test" "test"
      (do
        (println (cluster-name) (partitioner) (snitch))
        (insert {:key "blah" :name "name" :value "bailey"})
        (get {:key "blah" :name "name"})))))
