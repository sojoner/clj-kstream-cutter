(ns clj-kstream-cutter.test-utils
  (:use [clojure.tools.logging :only (debug info error warn)])
  (:require
    [clojure.spec.gen :as gen]
    [clojure.spec :as s]
    [clojure.data.json :as json])
  (:import (java.util Properties)
           (scala Option)
           (kafka.utils SystemTime$)
           (kafka.server KafkaConfig KafkaServer)
           (java.util.concurrent Executors)
           (org.apache.zookeeper.server ServerConfig ZooKeeperServerMain)
           (org.apache.zookeeper.server.quorum QuorumPeerConfig)
           (org.apache.kafka.streams StreamsConfig)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback RecordMetadata)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords)))

(def kafka "127.0.0.1:9092")
(def zookeeper "127.0.0.1:2181")

(defn- get-zookeeper-props []
  "The zookeeper properties"
  (doto (new Properties)
    (.put "clientPort" "2181")
    (.put "dataDir" "./target/")
    (.put "tickTime", "1000")))

(defn- get-broker-props []
  "The kafka properties"
  (doto (new Properties)
       (.put "zookeeper.connect" zookeeper)
       (.put "log.dir" "./target/kafka-logs")))

(defn- get-producer-props []
  (doto (new Properties)
    (.put "bootstrap.servers" kafka)
    (.put "key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    (.put "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")))

(defn- get-consumer-props []
  (doto (new Properties)
    (.put "group.id", "consumer-test");
    (.put "bootstrap.servers" kafka)
    (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "key.deserializer"  "org.apache.kafka.common.serialization.StringDeserializer")))

(s/def ::first string?)
(s/def ::last string?)
(s/def ::person (s/keys :req [::first ::last]))

(defn produce-fake-json [topic-name number-of-sample]
  (let [producer (KafkaProducer. (get-producer-props))]
    (doseq [idx (range number-of-sample)]
        (let [raw-data (gen/generate (s/gen ::person))
              producer_record (ProducerRecord.
                                topic-name
                                (:first raw-data)
                                (json/write-str raw-data))]
          (.send producer
                 producer_record
                 (reify Callback
                   (^void onCompletion [this ^RecordMetadata var1, ^Exception var2]
                     (when var2
                       (error "Error" var2))
                     (when var1
                       (info "Info" (.offset var1))))))))))

(defn ^ConsumerRecords poll-from-topic [topic-name number-of-items]
  (let[consumer-props (get-consumer-props)
       consumer (KafkaConsumer. consumer-props)
       _ (.subscribe consumer (list topic-name))]
    (doseq [idx (range number-of-items)]
      (let [poll (.poll consumer number-of-items)]
        (info "POLL::" poll)
        (doseq [item poll]
          (info "Consumed: " (:key item) (:value item)))))))

(defn start-zookeeper []
  (let [qpConfig (QuorumPeerConfig.)
        zookeeperProps (get-zookeeper-props)
        _ (.parseProperties qpConfig zookeeperProps)
        zkServer (ZooKeeperServerMain.)
        zkServerConfig (ServerConfig.)
        _ (.readFrom zkServerConfig qpConfig)
        excuter (Executors/newCachedThreadPool)]
    (.submit excuter (reify Callable
                       (call [this]
                         (.runFromConfig zkServer zkServerConfig))))))

(defn start-kafka []
  (let [server (KafkaServer.
                 (KafkaConfig. (get-broker-props))
                 (SystemTime$/MODULE$)
                 (Option/apply "test-core"))]
    (.startup server)))