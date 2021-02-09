# resurfaceio-trino-connector
Custom connector for Trino

## System requirements

* Java 11
* Maven

## Configuring local environment

This connector is built on Jenkins and packaged into our containers, but it's easy to build/deploy a local version.

```
1. Install Trino
download and expand tarball to local directory
define TRINO_HOME environment variable

2. Create $TRINO_HOME/etc/catalog/resurface.properties:
connector.name=resurface

3. Build the connector
mvn package

4. Copy output to Trino
rm -rf $TRINO_HOME/plugin/resurface
cp -r ./target/resurfaceio-trino-connector-2.1.2 $TRINO_HOME/plugin/resurface

5. Start Trino
bash bin/launcher run
```