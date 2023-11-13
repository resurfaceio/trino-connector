// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.resurface.trino.connector.ResurfaceTables.MessageTable.getSchemaTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.*;

public class TestResurfaceRecordSet {

    @Test
    public void testCompressedFiles() {
        String location = getClass().getClassLoader().getResource("compressed-files").getPath();
        ResurfaceTables tables = new ResurfaceTables(new ResurfaceConfig().setMessagesDir(location));
        ResurfaceMetadata metadata = new ResurfaceMetadata(tables);

        ResurfaceTableHandle tableHandle = new ResurfaceTableHandle(getSchemaTableName());
        List<ResurfaceColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle)
                .values().stream().map(column -> (ResurfaceColumnHandle) column)
                .collect(Collectors.toList());

        ResurfaceSplit split = new ResurfaceSplit(HostAddress.fromParts("localhost", 1234), "test_node_id", 1);
        RecordSet recordSet = new ResurfaceRecordSet(tables, split, tableHandle, columnHandles);
        try (RecordCursor cursor = recordSet.cursor()) {
            for (int i = 0; i < columnHandles.size(); i++) {
                assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
            }

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getSlice(0).toStringUtf8(), "id 😀");                                   // 0
            assertEquals(cursor.getSlice(1).toStringUtf8(), "agent_category 😀");                       // 1
            assertEquals(cursor.getSlice(2).toStringUtf8(), "agent_device 😀");                         // 2
            assertEquals(cursor.getSlice(3).toStringUtf8(), "agent_name 😀");                           // 3
            assertEquals(cursor.getSlice(4).toStringUtf8(), "graphql_operations 😀");                   // 4 (v3)
            assertEquals(cursor.getLong(5), 27);                                                        // 5 (v3)
            assertEquals(cursor.getSlice(6).toStringUtf8(), "host 😀");                                 // 6
            assertEquals(cursor.getLong(7), 123456);                                                    // 7
            assertEquals(cursor.getSlice(8).toStringUtf8(), "request_body 😀");                         // 8
            assertEquals(cursor.getSlice(9).toStringUtf8(), "request_content_type 😀");                 // 9
            assertEquals(cursor.getSlice(10).toStringUtf8(), "request_headers 😀");                     // 10
            assertEquals(cursor.getSlice(11).toStringUtf8(), "request_json_type 😀");                   // 11
            assertEquals(cursor.getSlice(12).toStringUtf8(), "request_method 😀");                      // 12
            assertEquals(cursor.getSlice(13).toStringUtf8(), "request_params 😀");                      // 13
            assertEquals(cursor.getSlice(14).toStringUtf8(), "request_url 😀");                         // 14
            assertEquals(cursor.getSlice(15).toStringUtf8(), "request_user_agent 😀");                  // 15
            assertEquals(cursor.getSlice(16).toStringUtf8(), "response_body 😀");                       // 16
            assertEquals(cursor.getSlice(17).toStringUtf8(), "response_code 😀");                       // 17
            assertEquals(cursor.getSlice(18).toStringUtf8(), "response_content_type 😀");               // 18
            assertEquals(cursor.getSlice(19).toStringUtf8(), "response_headers 😀");                    // 19
            assertEquals(cursor.getSlice(20).toStringUtf8(), "response_json_type 😀");                  // 20
            assertEquals(cursor.getLong(21), 1234);                                                     // 21
            assertEquals(cursor.getLong(22), 23);                                                       // 22
            assertEquals(cursor.getLong(23), 45);                                                       // 23
            assertEquals(cursor.getSlice(24).toStringUtf8(), "custom_fields 😀");                       // 24 (v3)
            assertEquals(cursor.getSlice(25).toStringUtf8(), "request_address 😀");                     // 25 (v3)
            assertEquals(cursor.getSlice(26).toStringUtf8(), "session_fields 😀");                      // 26 (v3)
            assertEquals(cursor.getSlice(27).toStringUtf8(), "cookies 😀");                             // 27 (v3)
            assertEquals(cursor.getLong(28), 56);                                                       // 28 (v3)
            assertEquals(cursor.getSlice(29).toStringUtf8(), "");                                       // 29 (v3.1)
            assertEquals(cursor.getLong(30), 68);                                                       // 30 (v3.1)
            assertEquals(cursor.getLong(31), 31);                                                       // 31 (v3.1)
            assertEquals(cursor.getLong(32), 32);                                                       // 32 (v3.1)
            assertEquals(cursor.getLong(33), 33);                                                       // 33 (v3.1)
            assertEquals(cursor.getLong(34), 34);                                                       // 34 (v3.1)
            assertEquals(cursor.getLong(35), 35);                                                       // 35 (v3.1)
            assertEquals(cursor.getLong(36), 36);                                                       // 36 (v3.1)
            assertEquals(cursor.getLong(37), 37);                                                       // 37 (v3.1)
            assertEquals(cursor.getLong(38), 38);                                                       // 38 (v3.1)
            assertEquals(cursor.getLong(39), 39);                                                       // 39 (v3.1)
            assertEquals(cursor.getLong(40), 40);                                                       // 40 (v3.1)
            assertEquals(cursor.getLong(41), 41);                                                       // 41 (v3.1)
            assertEquals(cursor.getLong(42), 42);                                                       // 42 (v3.1)
            assertEquals(cursor.getLong(43), 43);                                                       // 43 (v3.1)
            assertEquals(cursor.getLong(44), 44);                                                       // 44 (v3.1)
            assertEquals(cursor.getLong(45), 45);                                                       // 45 (v3.1)
            assertEquals(cursor.getLong(46), 46);                                                       // 46 (v3.1)
            assertEquals(cursor.getLong(47), 47);                                                       // 47 (v3.1)
            assertEquals(cursor.getLong(48), 48);                                                       // 48 (v3.1)
            assertEquals(cursor.getSlice(49).toStringUtf8(), "message.1.blkc");                         // 49 (v3.5)
            assertEquals(cursor.getLong(50), 9);                                                        // 50 (v3.6)
            assertEquals(cursor.getLong(51), 17);                                                       // 51 (v3.6)
            assertEquals(cursor.getLong(52), 20);                                                       // 52 (v3.6)
            assertEquals(cursor.getLong(53), 19);                                                       // 53 (v3.6)
            assertEquals(cursor.getLong(54), 16);                                                       // 54 (v3.6)
            assertEquals(cursor.getLong(55), 18);                                                       // 55 (v3.6)
            assertEquals(cursor.getLong(56), 21);                                                       // 56 (v3.6)
            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getSlice(0).toStringUtf8(), "id2");

            assertFalse(cursor.advanceNextPosition());
        }
    }

}
