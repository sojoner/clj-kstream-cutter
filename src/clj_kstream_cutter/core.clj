(ns clj-kstream-cutter.core
  (:use [clojure.tools.logging :only (debug info error warn)])
  (:require [clojure.data.json :as json]
            [clj-kstream-cutter.cli :as cli-def]
            [clojure.tools.cli :as cli])

  (:import
    (kafka.utils ZKStringSerializer$ ZkUtils)
    (kafka.admin AdminUtils RackAwareMode RackAwareMode$Enforced$)
    (org.apache.kafka.streams KafkaStreams
                              StreamsConfig KeyValue)
    (org.apache.kafka.streams.kstream KStream
                                      KStreamBuilder
                                      KTable KeyValueMapper ForeachAction ValueMapper)
    (org.apache.kafka.streams.processor AbstractProcessor)
    (org.apache.kafka.common.serialization Deserializer
                                           Serde
                                           Serdes
                                           Serializer)
    (org.apache.kafka.common.serialization StringDeserializer
                                           StringSerializer)
    (java.util Properties)
    (java.util.function Function)
    (org.I0Itec.zkclient ZkClient ZkConnection))
  (:gen-class))

(def string_ser
  "The Serializer"
  (StringSerializer.))

(def string_dser
  "The de-serializer"
  (StringDeserializer.))

(def stringSerde
  "The serialization pair"
  (Serdes/serdeFrom string_ser string_dser))

(defn- get-props [conf]
  "The kafka properties"
  (doto (new Properties)
    (.put StreamsConfig/APPLICATION_ID_CONFIG (:name conf))
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG (:kafka-brokers conf))
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG (:zookeeper-servers conf))))

(defn split-string-value-of-dict [data selector]
  "Select a value given by the selector path, and split the string at the white space."
  (try
    (let [field-value (get-in data (into [] (first selector)))]
      (map #(clojure.string/trim %1)
           (clojure.string/split field-value #"\s")))
    (catch Exception e
      (error "Failed parsing field: " selector e)
      (list))))

(defn- stream-mapper
  "Main stream processor takes a configuration and a mapper function to apply."
  [conf ]
  (let [streamBuilder (KStreamBuilder.)
        ^KStream log-stream (.stream
                              streamBuilder
                              stringSerde
                              stringSerde
                              (into-array String [(:input-topic conf)]))]
    (-> log-stream
        (.flatMapValues (reify ValueMapper
                          (apply [this value]
                            (try
                               (let [value-as-dict (json/read-str value :key-fn keyword)]
                                 (split-string-value-of-dict value-as-dict (:selector conf) ))
                               (catch Exception e
                                 (error "Failed parsing .json" e)
                                 (list))))))
        (.map  (reify KeyValueMapper
                 (apply [this k v]
                   (KeyValue. v v))))
        (.through stringSerde stringSerde (:output-topic conf)))

    (.start (KafkaStreams. streamBuilder (get-props conf)))))

(defn check-topic [zookeeper topic]
  (info "ZK:" zookeeper " Topic: " topic)
  (try
     (with-open [zkClient (new ZkClient zookeeper 10000 8000 ZKStringSerializer$/MODULE$)]
       (let [zkUtils (new ZkUtils zkClient (new ZkConnection zookeeper), false)]
         (AdminUtils/createTopic zkUtils topic, 1, 1, (new Properties), RackAwareMode$Enforced$/MODULE$)))
     (catch Exception e
       (error "Failed to create topic" e))))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-def/cli-options)
        conf {:kafka-brokers         (:broker options)
              :zookeeper-servers   (:zookeeper options)
              :input-topic (:input-topic options)
              :output-topic   (:output-topic options)
              :selector (:selector options)
              :name (:name options)}]
    (cond
      (:help options) (cli-def/exit 0 (cli-def/usage summary))
      (not= (count (keys options)) 6) (cli-def/exit 1 (cli-def/usage summary))
      (not (nil? errors)) (cli-def/exit 1 (cli-def/error-msg errors)))
    (check-topic (:zookeeper-servers conf)(:input-topic conf))
    (check-topic (:zookeeper-servers conf)(:output-topic conf))
    (stream-mapper conf)))
