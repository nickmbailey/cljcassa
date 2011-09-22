(defproject cljcassa "0.1-SNAPSHOT"
  :source-path "src/"
  :main cljcassa.core
  :test-path "test"
  :description "Clojure client for Apache Cassandra"
  :library-path "lib"
  :warn-on-reflection true
  :min-lein-verion "1.6.1"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.apache.thrift/libthrift "0.6.1"]
                 [org.apache.cassandra/cassandra-thrift "0.8.4"]
                 [log4j/log4j "1.2.16"]
                 [org.slf4j/slf4j-log4j12 "1.6.2"]
                 [org.slf4j/slf4j-api "1.6.2"]])
