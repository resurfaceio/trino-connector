#!/bin/bash

mv target/resurfaceio-trino-connector-$1.zip target/resurfaceio-trino-connector.zip
cloudsmith push raw resurfacelabs/public target/resurfaceio-trino-connector.zip --version $1
