#!/bin/bash
mv ../target/clj-kstream-cutter.jar .
docker build --tag "sojoner/clj-kstream-cutter:0.1.0" .
