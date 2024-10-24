# resurfaceio-trino-connector
Custom connector for Trino

This open source Java library allows [Trino](https://trino.io) to run [SQL queries](https://resurface.io/docs#sql-reference)
against [Resurface binary files](https://github.com/resurfaceio/binfiles). This connector provides table and column
definitions, virtual column definitions, helpful custom functions, and management for persistent and volatile views.
This connector can be configured for parallel splits, where multiple binary files are read in parallel for best performance.

[![CodeFactor](https://www.codefactor.io/repository/github/resurfaceio/trino-connector/badge)](https://www.codefactor.io/repository/github/resurfaceio/trino-connector)
[![Contributing](https://img.shields.io/badge/contributions-welcome-green.svg)](https://github.com/resurfaceio/trino-connector/blob/v3.7.x/CONTRIBUTING.md)
[![License](https://img.shields.io/github/license/resurfaceio/trino-connector)](https://github.com/resurfaceio/trino-connector/blob/v3.7.x/LICENSE)
[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat-square)](https://cloudsmith.io/~resurfaceio/repos/public/packages/)

## Usage

This connector is included with the Resurface database, but can be installed into any Trino distribution.

⚠️ We publish our official binaries on [CloudSmith](https://cloudsmith.io/~resurfaceio/repos/public/packages/) rather than Maven Central,
because CloudSmith is awesome and **free** for open-source projects.

## Dependencies

* Java 23
* Trino 463
* [resurfaceio/binfiles](https://github.com/resurfaceio/binfiles)

## Configuring Local Environment

```
1. Install Trino
download and expand tarball to local directory
export TRINO_HOME=$HOME/...

2. Create $TRINO_HOME/etc/catalog/resurface.properties:
connector.name=resurface

3. Build the connector and redeploy
mvn clean package && rm -rf $TRINO_HOME/plugin/resurface && cp -r ./target/resurfaceio-trino-connector-3.7.3 $TRINO_HOME/plugin/resurface

4. Start Trino
cd $TRINO_HOME
bash bin/launcher run
```

---
<small>&copy; 2016-2024 <a href="https://resurface.io">Graylog, Inc.</a></small>
