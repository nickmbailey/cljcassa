(ns cljcassa.columnfamily
  (import java.nio.ByteBuffer)
  (import java.nio.charset.Charset)
  (gen-class))

(def charset (Charset/forName "UTF8"))

(defn create-cf [name cfdefs]
  (if-let [cfdef (first (filter #(= name (:name %)) (map bean cfdefs)))]
    cfdef
    (throw (new Exception (str "Column Family " name " does not exist")))))

(defmulti ser-key (fn [key cf] (:key_validation_class cf)))
(defmethod ser-key "org.apache.cassandra.db.marshal.UTF8Type" [key -]
  (ByteBuffer/wrap (.getBytes key)))
(defmethod ser-key :default [key _] (throw (new Exception "crap")))

(defmulti ser-col-name (fn [col cf] (:comparator_type cf)))
(defmethod ser-col-name "org.apache.cassandra.db.marshal.UTF8Type" [col _]
  (ByteBuffer/wrap (.getBytes (:name col))))
(defmethod ser-col-name :default [col _] (throw (new Exception "crap")))

(defmulti ser-col-value (fn [col cf] (:default_validation_class cf)))
(defmethod ser-col-value "org.apache.cassandra.db.marshal.UTF8Type" [col _]
  (ByteBuffer/wrap (.getBytes (:value col))))
(defmethod ser-col-value :default [col _] (throw (new Exception "crap")))

(defmulti deser-col-name (fn [col cf] (:comparator_type cf)))
(defmethod deser-col-name "org.apache.cassandra.db.marshal.UTF8Type" [col _]
  (assoc col :name (new String (:name col))))
(defmethod deser-col-name :default [col _] (throw (new Exception "crap")))

(defmulti deser-col-value (fn [col cf] (:default_validation_class cf)))
(defmethod deser-col-value "org.apache.cassandra.db.marshal.UTF8Type" [col _]
  (assoc col :value (new String (:value col))))
(defmethod deser-col-value :default [col _] (throw (new Exception "crap")))


(def filterable #{:class :setValue :setTtl :setTimestamp :setName})
(defn- filter-col [col]
  (apply dissoc col filterable))

(defn deserialize-column [cf col]
  (let [col (bean (.column col))]
    (-> col
      (deser-col-name cf)
      (deser-col-value cf)
      (filter-col))))
