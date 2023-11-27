// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.resurface.trino.connector.HistosumStateSerializer.toJSON;
import static io.trino.spi.type.VarcharType.VARCHAR;

@AggregationFunction("histosum")
@Description("Sum of values grouped by key")
public final class Histosum {

    private Histosum() {}

    @InputFunction
    public static void input(@AggregationState HistosumState state, @SqlType(StandardTypes.VARCHAR) Slice key, @SqlType(StandardTypes.VARCHAR) Slice value) {
        Map<String, Object> m = state.getMap();
        String k = key.toStringUtf8();
        long v = Long.parseLong(value.toStringUtf8());

        if (m == null) {
            LinkedHashMap<String, Object> seed = new LinkedHashMap<>();
            seed.put(k, v);
            state.setMap(seed);
        } else {
            Object existing = m.getOrDefault(k, 0L);
            m.put(k, v + ((Number) existing).longValue());
        }
    }

    @CombineFunction
    public static void combine(@AggregationState HistosumState s1, @AggregationState HistosumState s2) {
        Map<String, Object> m1 = s1.getMap();
        Map<String, Object> m2 = s2.getMap();

        if (m1 != null && m2 != null) {
            s1.setMap(merge(m1, m2));
        } else if (m1 == null) {
            s1.setMap(m2);
        }
    }

    public static Map<String, Object> merge(Map<String, Object> m1, Map<String, Object> m2) {
        return Stream.of(m1, m2)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> ((Number) v1).longValue() + ((Number) v2).longValue()));
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(@AggregationState HistosumState state, BlockBuilder out) {
        Map<String, Object> m = state.getMap();
        if (m == null) {
            out.appendNull();
        } else {
            VARCHAR.writeString(out, toJSON(m));
        }
    }

}
