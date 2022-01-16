# resurfaceio-trino-connector
Custom connector for Trino

## System requirements

* Java 11
* Maven

## Configuring local environment

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
cp -r ./target/resurfaceio-trino-connector-3.0.14 $TRINO_HOME/plugin/resurface

5. Start Trino
bash bin/launcher run
```