// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static io.resurface.trino.connector.MetadataUtil.COLUMN_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestResurfaceColumnHandle {

    private final List<ResurfaceColumnHandle> columnHandle = ImmutableList.of(
            new ResurfaceColumnHandle("columnName", createUnboundedVarcharType(), 0),
            new ResurfaceColumnHandle("columnName", BIGINT, 0),
            new ResurfaceColumnHandle("columnName", DOUBLE, 0),
            new ResurfaceColumnHandle("columnName", DATE, 0),
            new ResurfaceColumnHandle("columnName", createTimestampWithTimeZoneType(3), 0),
            new ResurfaceColumnHandle("columnName", BOOLEAN, 0));

    @Test
    public void testJsonRoundTrip() {
        for (ResurfaceColumnHandle handle : columnHandle) {
            String json = COLUMN_CODEC.toJson(handle);
            ResurfaceColumnHandle copy = COLUMN_CODEC.fromJson(json);
            assertEquals(copy, handle);
        }
    }

}
