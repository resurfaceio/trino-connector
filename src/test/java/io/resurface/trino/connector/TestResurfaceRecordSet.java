// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.resurface.trino.connector.ResurfaceTables.MessageTable.getSchemaTableName;
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

//        assertData(tables, metadata);
    }

    private static void assertData(ResurfaceTables tables, ResurfaceMetadata metadata) {
        ResurfaceTableHandle tableHandle = new ResurfaceTableHandle(getSchemaTableName());
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
//        assertEquals(cursor.getSlice(1).toStringUtf8(), "192.168.4.61");
//        assertEquals(cursor.getSlice(2).toStringUtf8(), "POST");
//        assertEquals(cursor.getSlice(3).toStringUtf8(), "/v1/memory");
//        assertEquals(cursor.getSlice(4).toStringUtf8(), "rdickinson");
//        assertEquals(cursor.getSlice(5).toStringUtf8(), "myagent");
//        assertEquals(cursor.getLong(6), 200);
//        assertEquals(cursor.getLong(7), 73);
//        assertEquals(cursor.getLong(8), 269);
//        assertEquals(cursor.getLong(9), 2);
//        assertEquals(cursor.getSlice(10).toStringUtf8(), "asdf1234");
    }

    private String getResourceFilePath(String fileName) {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

}
