# clj-kstream-cutter

A Clojure app to process one kafka topic. Select a .json
field by providing a path like *root.substructure.fieldname* and split this into a list of token.

## Requirements

* [leiningen 2.7.1](https://leiningen.org/)
* [kafka 0.10.0.1](http://kafka.apache.org) 
* [docker 1.12.6](https://www.docker.com/)

### Kafka Topic

The topic you are consuming needs to have **^String** Keys and **^String** .json values.  

## Build Clojure
    
    $lein check

## Build .jar

    $lein uberjar

## Build .container
    
    $cd deploy
    $./containerize.sh

## Usage Leiningen

    $lein check
    $lein run --broker kafka-broker:9092 --zookeeper zookeeper:2181 --input-topic logs-replay --output-topic mapped-test-json --selector msg --name stream-cut-json-field
## Usage java

    $java - jar clj-kstream-cutter.jar --broker kafka-broker:9092 --zookeeper zookeeper:2181 --input-topic logs-replay --output-topic mapped-test-json --selector msg --name stream-cut-json-field

## Usage docker

    $docker run -t -i <BUILD-HASH> --broker kafka-broker:9092 --zookeeper zookeeper:2181 --input-topic logs-replay --output-topic mapped-test-json --selector msg --name stream-cut-json-field


## License

Copyright © 2017 Sojoner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
