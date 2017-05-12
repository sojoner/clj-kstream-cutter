(ns clj-kstream-cutter.core-test
  (:require [clojure.test :refer :all]
            [clj-kstream-cutter.test-utils :as test-utils]
            [clj-kstream-cutter.core :refer :all])
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig)
           (org.apache.zookeeper.server.quorum QuorumPeerConfig)
           (org.apache.zookeeper.server ZooKeeperServerMain ServerConfig)
           (java.util.concurrent ExecutorService Executors)
           (kafka.server KafkaServer KafkaConfig)
           (kafka.utils SystemTime$)
           (scala.Option)
           (scala Option)))



(deftest test-json-cutting-messages
  (testing "Test cutter"
    (test-utils/start-zookeeper)
    (test-utils/start-kafka)
    (let[conf {:kafka-brokers  "127.0.0.1:9092"
               :input-topic "test-cutter-in"
               :output-topic "test-cutter-out"
               :selector (list (map #(keyword %1) (clojure.string/split "first." #"\.")))
               :name "test-name"}
         stream (stream-mapper conf)
         _ (test-utils/produce-fake-json (:input-topic conf) 100)
         _ (test-utils/poll-from-topic (:output-topic conf) 100)]
      (is (= 100 100)))))
