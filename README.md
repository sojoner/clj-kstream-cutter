# clj-kstream-cutter

A Clojure app to process one kafka topics. Select a .json
field and split this into a list of token.

## Requirements

* Your topics have .json values

## Usage

    $lein check
    $lein run --broker kafka-broker:9092 --zookeeper zookeeper:2181 --input-topic logs-replay --output-topic mapped-test-json --selector msg --name stream-test

## License

Copyright © 2016 Hagen Tönnies

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
