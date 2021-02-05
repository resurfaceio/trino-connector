#!/bin/bash

mvn clean
mvn package
mv target/resurfaceio-trino-connector-$1.zip target/resurfaceio-trino-connector.zip
cloudsmith push raw resurfacelabs/internal target/resurfaceio-trino-connector.zip --version $1
