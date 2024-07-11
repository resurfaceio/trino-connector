// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class HistosumStateSerializer implements AccumulatorStateSerializer<HistosumState> {

    @Override
    public void deserialize(Block block, int index, HistosumState state) {
        Slice slice = VARCHAR.getSlice(block, index);
        try {
            state.setMap(new LinkedHashMap<>(OBJECT_MAPPER.readValue(slice.getBytes(), new TypeReference<LinkedHashMap<String, Double>>() {})));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Type getSerializedType() {
        return VARCHAR;
    }

    @Override
    public void serialize(HistosumState state, BlockBuilder out) {
        if (state.getMap() == null) {
            out.appendNull();
        } else {
            VARCHAR.writeSlice(out, Slices.utf8Slice(toJSON(state.getMap())));
        }
    }

    public static String toJSON(Map<String, Double> map) {
        try {
            ObjectNode node = OBJECT_MAPPER.createObjectNode();
            for (Map.Entry<String, Double> e : map.entrySet()) node.put(e.getKey(), e.getValue());
            return OBJECT_MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

}
