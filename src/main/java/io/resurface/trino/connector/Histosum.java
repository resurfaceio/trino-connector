// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.resurface.trino.connector.HistosumStateSerializer.toJSON;
import static io.trino.spi.type.VarcharType.VARCHAR;

@AggregationFunction("histosum")
@Description("Sum of values grouped by key")
public final class Histosum {

    private Histosum() {}

    @InputFunction
    public static void input(@AggregationState HistosumState state, @SqlType(StandardTypes.VARCHAR) Slice key, @SqlType(StandardTypes.DOUBLE) double value) {
        Map<String, Double> m = state.getMap();
        String k = key.toStringUtf8();

        if (m == null) {
            LinkedHashMap<String, Double> seed = new LinkedHashMap<>();
            seed.put(k, value);
            state.setMap(seed);
        } else {
            m.put(k, value + m.getOrDefault(k, 0.0));
        }
    }

    @CombineFunction
    public static void combine(@AggregationState HistosumState s1, @AggregationState HistosumState s2) {
        Map<String, Double> m1 = s1.getMap();
        Map<String, Double> m2 = s2.getMap();

        if (m1 != null && m2 != null) {
            for (Map.Entry<String, Double> e : m2.entrySet()) {
                String k = e.getKey();
                m1.put(k, e.getValue() + m1.getOrDefault(k, 0.0));
            }
        } else if (m1 == null) {
            s1.setMap(m2);
        }
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(@AggregationState HistosumState state, BlockBuilder out) {
        Map<String, Double> m = state.getMap();
        if (m == null) {
            out.appendNull();
        } else {
            VARCHAR.writeString(out, toJSON(m));
        }
    }

}
