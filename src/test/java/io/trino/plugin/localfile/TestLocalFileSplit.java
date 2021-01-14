// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestLocalFileSplit
{
    private final HostAddress address = HostAddress.fromParts("localhost", 1234);
    private final LocalFileSplit split = new LocalFileSplit(address);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LocalFileSplit> codec = jsonCodec(LocalFileSplit.class);
        String json = codec.toJson(split);
        LocalFileSplit copy = codec.fromJson(json);

        assertEquals(copy.getAddress(), split.getAddress());

        assertEquals(copy.getAddresses(), ImmutableList.of(address));
        assertEquals(copy.isRemotelyAccessible(), false);
    }
}
