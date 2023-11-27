// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

import java.util.Map;

@AccumulatorStateMetadata(stateFactoryClass = HistosumStateFactory.class, stateSerializerClass = HistosumStateSerializer.class)
public interface HistosumState extends AccumulatorState {

    Map<String, Object> getMap();

    void setMap(Map<String, Object> value);

}
