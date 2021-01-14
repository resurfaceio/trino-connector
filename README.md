# resurfaceio-trino-connector
Custom connector for Trino

```
1. Create $TRINO_HOME/etc/catalog/resurface.properties:
connector.name=resurface

2. Build the connector
mvn package

3. Copy output to Trino
rm -rf $TRINO_HOME/plugin/resurface
cp ./target/resurfaceio-trino-connector-2.x.x $TRINO_HOME/plugin/resurface
```