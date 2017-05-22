#!/bin/bash
mv ../target/clj-kstream-cutter.jar .
docker build --tag "sojoner/clj-kstream-cutter:0.3.0" .
docker tag <HASH> sojoner/clj-kstream-cutter:0.3.0
docker login
docker push sojoner/clj-kstream-elasticsearch-sink