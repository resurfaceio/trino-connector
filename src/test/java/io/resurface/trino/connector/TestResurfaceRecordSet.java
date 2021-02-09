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
import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResurfaceRecordSet {

    @Test
    public void testExampleData() {
        String location = getClass().getClassLoader().getResource("example-data").getPath();
        ResurfaceTables tables = new ResurfaceTables(new ResurfaceConfig().setMessagesDir(location));
        ResurfaceMetadata metadata = new ResurfaceMetadata(tables);

        ResurfaceTableHandle tableHandle = new ResurfaceTableHandle(getSchemaTableName());
        List<ResurfaceColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle)
                .values().stream().map(column -> (ResurfaceColumnHandle) column)
                .collect(Collectors.toList());

        ResurfaceSplit split = new ResurfaceSplit(HostAddress.fromParts("localhost", 1234));
        RecordSet recordSet = new ResurfaceRecordSet(tables, split, tableHandle, columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
        }

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "id");
        assertEquals(cursor.getSlice(1).toStringUtf8(), "agent_category");
        assertEquals(cursor.getSlice(2).toStringUtf8(), "agent_device");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "agent_name");
        assertEquals(cursor.getSlice(4).toStringUtf8(), "host");
        assertEquals(cursor.getSlice(5).toStringUtf8(), "interval_category");
        assertEquals(cursor.getSlice(6).toStringUtf8(), "interval_clique");
        assertEquals(cursor.getLong(7), 123456);
        assertEquals(cursor.getSlice(8).toStringUtf8(), "request_body");
        assertEquals(cursor.getSlice(9).toStringUtf8(), "request_content_type");
        assertEquals(cursor.getSlice(10).toStringUtf8(), "request_headers");
        assertEquals(cursor.getSlice(11).toStringUtf8(), "request_method");
        assertEquals(cursor.getSlice(12).toStringUtf8(), "request_params");
        assertEquals(cursor.getSlice(13).toStringUtf8(), "request_url");
        assertEquals(cursor.getSlice(14).toStringUtf8(), "request_user_agent");
        assertEquals(cursor.getSlice(15).toStringUtf8(), "response_body");
        assertEquals(cursor.getSlice(16).toStringUtf8(), "response_code");
        assertEquals(cursor.getSlice(17).toStringUtf8(), "response_content_type");
        assertEquals(cursor.getSlice(18).toStringUtf8(), "response_headers");
        assertEquals(cursor.getLong(19), 1234);
        assertEquals(cursor.getSlice(20).toStringUtf8(), "size_category");
        assertEquals(cursor.getLong(21), 23);
        assertEquals(cursor.getLong(22), 45);

        assertFalse(cursor.advanceNextPosition());
    }

}