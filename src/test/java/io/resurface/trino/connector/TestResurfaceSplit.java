// © 2016-2023 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestResurfaceSplit {

    private final HostAddress address = HostAddress.fromParts("localhost", 1234);
    private final ResurfaceSplit split = new ResurfaceSplit(address, "test_node_id", 1);

    @Test
    public void testJsonRoundTrip() {
        JsonCodec<ResurfaceSplit> codec = jsonCodec(ResurfaceSplit.class);
        String json = codec.toJson(split);
        ResurfaceSplit copy = codec.fromJson(json);

        assertEquals(copy.getAddress(), split.getAddress());
        assertEquals(copy.getAddresses(), ImmutableList.of(address));
        assertEquals(copy.getNodeId(), split.getNodeId());
        assertEquals(copy.getSlab(), split.getSlab());
        assertEquals(copy.isRemotelyAccessible(), false);
    }

}
