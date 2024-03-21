#!/usr/bin/env bash

java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/perf-jdbc-0.1.0.jar
