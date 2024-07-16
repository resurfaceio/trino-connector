# resurfaceio-trino-connector
Custom connector for Trino

This open source Java library allows [Trino](https://trino.io) to run [SQL queries](https://resurface.io/docs#sql-reference)
against [Resurface binary files](https://github.com/resurfaceio/binfiles). This connector provides table and column
definitions, virtual column definitions, helpful custom functions, and management for persistent and volatile views.
This connector can be configured for parallel splits, where multiple binary files are read in parallel for best performance.

[![CodeFactor](https://www.codefactor.io/repository/github/resurfaceio/trino-connector/badge)](https://www.codefactor.io/repository/github/resurfaceio/trino-connector)
[![License](https://img.shields.io/github/license/resurfaceio/trino-connector)](https://github.com/resurfaceio/trino-connector/blob/v3.6.x/LICENSE)
[![Contributing](https://img.shields.io/badge/contributions-welcome-green.svg)](https://github.com/resurfaceio/trino-connector/blob/v3.6.x/CONTRIBUTING.md)

## Usage

This connector is included with the Resurface database, but can be installed
into any Trino 407+ distribution.

## Dependencies

* Java 22
* Trino SPI
* [resurfaceio/binfiles](https://github.com/resurfaceio/binfiles)

## Configuring Local Environment

```
1. Install Trino
download and expand tarball to local directory
export TRINO_HOME=$HOME/...

2. Create $TRINO_HOME/etc/catalog/resurface.properties:
connector.name=resurface

3. Build the connector and redeploy
mvn clean package && rm -rf $TRINO_HOME/plugin/resurface && cp -r ./target/resurfaceio-trino-connector-3.6.25 $TRINO_HOME/plugin/resurface

4. Start Trino
cd $TRINO_HOME
bash bin/launcher run
```

---
<small>&copy; 2016-2024 <a href="https://resurface.io">Graylog, Inc.</a></small>
