(defproject clj-kstream-cutter "0.3.0"
  :description "A Clojure app to process one kafka topics. Select a .json\nfield and split this into a list of token."
  :url "git@github.com:sojoner/clj-kstream-cutter.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.5"]
                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka_2.10
                 [org.apache.kafka/kafka_2.10 "0.10.0.1"]
                 [com.101tec/zkclient "0.9"]
                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
                 [org.apache.kafka/kafka-streams "0.10.0.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  :aot :all
  :main clj-kstream-cutter.core
  :uberjar-name "clj-kstream-cutter.jar"
  :profiles {:uberjar {:aot :all}}
  :jvm-opts ["-Xmx2g" "-server"])
