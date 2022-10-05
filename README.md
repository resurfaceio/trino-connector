# resurfaceio-trino-connector
Custom connector for Trino

This open source Java library allows [Trino](https://trino.io) to run SQL queries against
[Resurface binary files](https://github.com/resurfaceio/binfiles). This connector
provides table and column definitions, helpful custom functions, and management for
persistent and volatile view definitions. This connector is installed as part of
the Resurface database, but can also be used as an independent component.

[![CodeFactor](https://www.codefactor.io/repository/github/resurfaceio/trino-connector/badge)](https://www.codefactor.io/repository/github/resurfaceio/trino-connector)
[![License](https://img.shields.io/github/license/resurfaceio/trino-connector)](https://github.com/resurfaceio/trino-connector/blob/v3.3.x/LICENSE)
[![Contributing](https://img.shields.io/badge/contributions-welcome-green.svg)](https://github.com/resurfaceio/trino-connector/blob/v3.3.x/CONTRIBUTING.md)

## Dependencies

* Java 17
* Trino SPI
* [resurfaceio/binfiles](https://github.com/resurfaceio/binfiles)

## Usage

This connector is included with the Resurface database, but can be installed
into any Trino distribution.

## Configuring Local Environment

```
1. Install Trino
download and expand tarball to local directory
export TRINO_HOME=$HOME/...

2. Create $TRINO_HOME/etc/catalog/resurface.properties:
connector.name=resurface

3. Build the connector
mvn package

4. Copy output to Trino
rm -rf $TRINO_HOME/plugin/resurface
cp -r ./target/resurfaceio-trino-connector-3.3.5 $TRINO_HOME/plugin/resurface

5. Start Trino
bash bin/launcher run
```

---
<small>&copy; 2016-2022 <a href="https://resurface.io">Resurface Labs Inc.</a></small>
