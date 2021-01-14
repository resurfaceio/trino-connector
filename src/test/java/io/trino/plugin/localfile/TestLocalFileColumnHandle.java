// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.localfile.MetadataUtil.COLUMN_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestLocalFileColumnHandle
{
    private final List<LocalFileColumnHandle> columnHandle = ImmutableList.of(
            new LocalFileColumnHandle("columnName", createUnboundedVarcharType(), 0),
            new LocalFileColumnHandle("columnName", BIGINT, 0),
            new LocalFileColumnHandle("columnName", DOUBLE, 0),
            new LocalFileColumnHandle("columnName", DATE, 0),
            new LocalFileColumnHandle("columnName", createTimestampWithTimeZoneType(3), 0),
            new LocalFileColumnHandle("columnName", BOOLEAN, 0));

    @Test
    public void testJsonRoundTrip()
    {
        for (LocalFileColumnHandle handle : columnHandle) {
            String json = COLUMN_CODEC.toJson(handle);
            LocalFileColumnHandle copy = COLUMN_CODEC.fromJson(json);
            assertEquals(copy, handle);
        }
    }
}
