// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class ResurfaceTables {

    @Inject
    public ResurfaceTables(ResurfaceConfig config) {
        location = new DataLocation(config.getMessagesDir());
        viewsDir = config.getViewsDir();

        ImmutableMap.Builder<SchemaTableName, ResurfaceTableHandle> tablesBuilder = ImmutableMap.builder();
        tablesBuilder.put(MessageTable.getSchemaTableName(), new ResurfaceTableHandle(MessageTable.getSchemaTableName()));
        tablesBuilder.put(ShardTable.getSchemaTableName(), new ResurfaceTableHandle(ShardTable.getSchemaTableName()));
        tables = tablesBuilder.build();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        tableColumnsBuilder.put(MessageTable.getSchemaTableName(), MessageTable.getColumns());
        tableColumnsBuilder.put(ShardTable.getSchemaTableName(), ShardTable.getColumns());
        tableColumns = tableColumnsBuilder.build();
    }

    private final DataLocation location;
    private final Map<SchemaTableName, ResurfaceTableHandle> tables;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final String viewsDir;

    public List<ColumnMetadata> getColumns(ResurfaceTableHandle tableHandle) {
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    public List<File> getFiles(ResurfaceTableHandle handle, int slab) {
        return location.files().stream()
                .filter(f -> !f.isHidden())
                .filter(f -> f.getName().startsWith("message." + slab))
                .filter(f -> f.getName().endsWith(".blkc"))
                .filter(f -> getPredicateTest(f, handle))
                .collect(Collectors.toList());
    }

    public String getOpenFile() {
        List<File> files = location.files().stream()
                .filter(f -> !f.isHidden())
                .filter(f -> f.getName().equals("open_shard"))
                .collect(Collectors.toList());

        if (files.size() == 0) {
            return null;
        } else if (files.size() == 1) {
            try {
                return Files.readString(files.get(0).toPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Unexpected number of active files: " + files.size());
        }
    }

    private boolean getPredicateTest(File f, ResurfaceTableHandle h) {
        Map<ColumnHandle, NullableValue> x = new HashMap<>();
        x.put(MessageTable.SHARD_FILE, new NullableValue(createUnboundedVarcharType(), utf8Slice(f.getName())));
        return h.getConstraint().asPredicate().test(x);
    }

    public ResurfaceTableHandle getTable(SchemaTableName tableName) {
        return tables.get(tableName);
    }

    public String getViewsDir() {
        return viewsDir;
    }

    public static final class MessageTable {

        public static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                new ColumnMetadata("id", createUnboundedVarcharType()),                            // 0
                new ColumnMetadata("agent_category", createUnboundedVarcharType()),                // 1
                new ColumnMetadata("agent_device", createUnboundedVarcharType()),                  // 2
                new ColumnMetadata("agent_name", createUnboundedVarcharType()),                    // 3
                new ColumnMetadata("graphql_operations", createUnboundedVarcharType()),            // 4 (v3)
                new ColumnMetadata("graphql_operations_count", INTEGER),                           // 5 (v3)
                new ColumnMetadata("host", createUnboundedVarcharType()),                          // 6
                new ColumnMetadata("interval_millis", BIGINT),                                     // 7
                new ColumnMetadata("request_body", createUnboundedVarcharType()),                  // 8
                new ColumnMetadata("request_content_type", createUnboundedVarcharType()),          // 9
                new ColumnMetadata("request_headers", createUnboundedVarcharType()),               // 10
                new ColumnMetadata("request_json_type", createUnboundedVarcharType()),             // 11
                new ColumnMetadata("request_method", createUnboundedVarcharType()),                // 12
                new ColumnMetadata("request_params", createUnboundedVarcharType()),                // 13
                new ColumnMetadata("request_url", createUnboundedVarcharType()),                   // 14
                new ColumnMetadata("request_user_agent", createUnboundedVarcharType()),            // 15
                new ColumnMetadata("response_body", createUnboundedVarcharType()),                 // 16
                new ColumnMetadata("response_code", createUnboundedVarcharType()),                 // 17
                new ColumnMetadata("response_content_type", createUnboundedVarcharType()),         // 18
                new ColumnMetadata("response_headers", createUnboundedVarcharType()),              // 19
                new ColumnMetadata("response_json_type", createUnboundedVarcharType()),            // 20
                new ColumnMetadata("response_time_millis", BIGINT),                                // 21
                new ColumnMetadata("size_request_bytes", INTEGER),                                 // 22
                new ColumnMetadata("size_response_bytes", INTEGER),                                // 23
                new ColumnMetadata("custom_fields", createUnboundedVarcharType()),                 // 24 (v3)
                new ColumnMetadata("request_address", createUnboundedVarcharType()),               // 25 (v3)
                new ColumnMetadata("session_fields", createUnboundedVarcharType()),                // 26 (v3)
                new ColumnMetadata("cookies", createUnboundedVarcharType()),                       // 27 (v3)
                new ColumnMetadata("cookies_count", INTEGER),                                      // 28 (v3)
                new ColumnMetadata("response_status", createUnboundedVarcharType()),               // 29 (v3.1)
                new ColumnMetadata("size_total_bytes", BIGINT),                                    // 30 (v3.1)
                new ColumnMetadata("bitmap_versioning", BIGINT),                                   // 31 (v3.1)
                new ColumnMetadata("bitmap_request_info", BIGINT),                                 // 32 (v3.1)
                new ColumnMetadata("bitmap_request_json", BIGINT),                                 // 33 (v3.1)
                new ColumnMetadata("bitmap_request_graphql", BIGINT),                              // 34 (v3.1)
                new ColumnMetadata("bitmap_request_pii", BIGINT),                                  // 35 (v3.1)
                new ColumnMetadata("bitmap_request_threat", BIGINT),                               // 36 (v3.1)
                new ColumnMetadata("bitmap_response_info", BIGINT),                                // 37 (v3.1)
                new ColumnMetadata("bitmap_response_json", BIGINT),                                // 38 (v3.1)
                new ColumnMetadata("bitmap_response_pii", BIGINT),                                 // 39 (v3.1)
                new ColumnMetadata("bitmap_response_threat", BIGINT),                              // 40 (v3.1)
                new ColumnMetadata("bitmap_attack_request", BIGINT),                               // 41 (v3.1)
                new ColumnMetadata("bitmap_attack_application", BIGINT),                           // 42 (v3.1)
                new ColumnMetadata("bitmap_attack_injection", BIGINT),                             // 43 (v3.1)
                new ColumnMetadata("bitmap_response_leak", BIGINT),                                // 44 (v3.1)
                new ColumnMetadata("bitmap_unused2", BIGINT),                                      // 45 (v3.1)
                new ColumnMetadata("bitmap_unused3", BIGINT),                                      // 46 (v3.1)
                new ColumnMetadata("bitmap_unused4", BIGINT),                                      // 47 (v3.1)
                new ColumnMetadata("bitmap_unused5", BIGINT),                                      // 48 (v3.1)
                new ColumnMetadata("shard_file", createUnboundedVarcharType())                     // 49 (v3.5)
        );

        public static final ColumnHandle SHARD_FILE = new ResurfaceColumnHandle("shard_file", createUnboundedVarcharType(), 49);

        public static final String TABLE_NAME = "message";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(ResurfaceMetadata.SCHEMA_DATA, TABLE_NAME);
        }

    }

    public static final class ShardTable {

        public static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                new ColumnMetadata("node_id", createUnboundedVarcharType()),                       // 0 (v3.5)
                new ColumnMetadata("shard_file", createUnboundedVarcharType()),                    // 1 (v3.5)
                new ColumnMetadata("last_modified", BIGINT),                                       // 2 (v3.5)
                new ColumnMetadata("length_bytes", BIGINT),                                        // 3 (v3.5)
                new ColumnMetadata("open_for_writes", BOOLEAN)                                     // 4 (v3.5)
        );

        public static final String TABLE_NAME = "shard";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(ResurfaceMetadata.SCHEMA_DATA, TABLE_NAME);
        }

    }

}
