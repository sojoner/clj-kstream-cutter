(ns clj-kstream-cutter.core
  (:use [clojure.tools.logging :only (debug info error warn)])
  (:require [clojure.data.json :as json]
            [clj-kstream-cutter.cli :as cli-def]
            [clojure.tools.cli :as cli])
  (:import (org.apache.kafka.streams KafkaStreams
                                     StreamsConfig KeyValue)
           (org.apache.kafka.streams.kstream KStream
                                             KStreamBuilder
                                             KTable KeyValueMapper ForeachAction)
           (org.apache.kafka.streams.processor AbstractProcessor)
           (org.apache.kafka.common.serialization Deserializer
                                                  Serde
                                                  Serdes
                                                  Serializer)
           (org.apache.kafka.common.serialization StringDeserializer
                                                  StringSerializer)
           (java.util Properties)
           (java.util.function Function))
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

(defn- stream-mapper
  "Main stream processor takes a configuration and a mapper function to apply."
  [conf mapper-function]
  (let [streamBuilder (KStreamBuilder.)
        ^KStream log-stream (.stream
                              streamBuilder
                              stringSerde
                              stringSerde
                              (into-array String [(:input-topic conf)]))
        ^KStream mapped-stream (.map log-stream (reify KeyValueMapper
                                                  (apply [this k v]
                                                    (apply mapper-function k v (:selector conf)))))
        ^KStream through (.through mapped-stream stringSerde stringSerde (:output-topic conf))
        ^KafkaStreams streams (KafkaStreams. streamBuilder (get-props conf))]
    (.start streams)))


(defn split-string-value-of-dict [data selector]
  "Select a value given by the selector path, and split the string at the white space."
  (try
     (let [field-value (get-in data (into [] (first selector)))]
       (info field-value)
       (clojure.string/split field-value #"\s"))
     (catch Exception e
       (error "Failed parsing field: " selector e)
       (list))))


(defn process-item [key value selector]
  "
  Function that will be applied to every item in the stream.
  * Read .json into .edn
  * Process edn data
  * Write serialize .edn back to .json
  "
  (info key value selector)
  (let [value-as-dict (json/read-str value :key-fn keyword)
        new-value (split-string-value-of-dict value-as-dict selector)]
    (KeyValue. key (json/write-str new-value))))


(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-def/cli-options)
        conf {:kafka-brokers         (:broker options)
              :zookeeper-servers   (:zookeeper options)
              :input-topic (:input-topic options)
              :output-topic   (:output-topic options)
              :selector (list (:selector options))
              :name (:name options)}]
    (cond
      (:help options) (cli-def/exit 0 (cli-def/usage summary))
      (not= (count (keys options)) 6) (cli-def/exit 1 (cli-def/usage summary))
      (not (nil? errors)) (cli-def/exit 1 (cli-def/error-msg errors)))
    (stream-mapper conf process-item)))
