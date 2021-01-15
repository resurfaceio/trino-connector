// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.resurface.trino.connector.ResurfaceTables.HttpRequestLogTable.getSchemaTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResurfaceRecordSet {

    private static final HostAddress address = HostAddress.fromParts("localhost", 1234);

    @Test
    public void testSimpleCursor() {
        String location = "example-data";
        ResurfaceTables tables = new ResurfaceTables(new ResurfaceConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        ResurfaceMetadata metadata = new ResurfaceMetadata(tables);

        assertData(tables, metadata);
    }

    @Test
    public void testGzippedData() {
        String location = "example-gzipped-data";
        ResurfaceTables tables = new ResurfaceTables(new ResurfaceConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        ResurfaceMetadata metadata = new ResurfaceMetadata(tables);

        assertData(tables, metadata);
    }

    private static void assertData(ResurfaceTables tables, ResurfaceMetadata metadata) {
        ResurfaceTableHandle tableHandle = new ResurfaceTableHandle(getSchemaTableName(), OptionalInt.of(0), OptionalInt.of(-1));
        List<ResurfaceColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle)
                .values().stream().map(column -> (ResurfaceColumnHandle) column)
                .collect(Collectors.toList());

        ResurfaceSplit split = new ResurfaceSplit(address);
        RecordSet recordSet = new ResurfaceRecordSet(tables, split, tableHandle, columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
        }

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), address.toString());
        assertEquals(cursor.getSlice(2).toStringUtf8(), "127.0.0.1");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "POST");
        assertEquals(cursor.getSlice(4).toStringUtf8(), "/v1/memory");
        assertTrue(cursor.isNull(5));
        assertTrue(cursor.isNull(6));
        assertEquals(cursor.getLong(7), 200);
        assertEquals(cursor.getLong(8), 0);
        assertEquals(cursor.getLong(9), 1000);
        assertEquals(cursor.getLong(10), 10);
        assertTrue(cursor.isNull(11));

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), address.toString());
        assertEquals(cursor.getSlice(2).toStringUtf8(), "127.0.0.1");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "GET");
        assertEquals(cursor.getSlice(4).toStringUtf8(), "/v1/service/presto/general");
        assertEquals(cursor.getSlice(5).toStringUtf8(), "foo");
        assertEquals(cursor.getSlice(6).toStringUtf8(), "ffffffff-ffff-ffff-ffff-ffffffffffff");
        assertEquals(cursor.getLong(7), 200);
        assertEquals(cursor.getLong(8), 0);
        assertEquals(cursor.getLong(9), 37);
        assertEquals(cursor.getLong(10), 1094);
        assertEquals(cursor.getSlice(11).toStringUtf8(), "a7229d56-5cbd-4e23-81ff-312ba6be0f12");
    }

    private String getResourceFilePath(String fileName) {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

}
