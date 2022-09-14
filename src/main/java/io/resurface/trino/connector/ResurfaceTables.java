// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class ResurfaceTables {

    @Inject
    public ResurfaceTables(ResurfaceConfig config) {
        location = new DataLocation(config.getMessagesDir());
        slabs = config.getMessagesSlabs();
        viewsDir = config.getViewsDir();

        SchemaTableName table = MessageTable.getSchemaTableName();
        ImmutableMap.Builder<SchemaTableName, ResurfaceTableHandle> tablesBuilder = ImmutableMap.builder();
        tablesBuilder.put(table, new ResurfaceTableHandle(table));
        tables = tablesBuilder.build();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        tableColumnsBuilder.put(table, MessageTable.getColumns());
        tableColumns = tableColumnsBuilder.build();
    }

    private final DataLocation location;
    private final int slabs;
    private final Map<SchemaTableName, ResurfaceTableHandle> tables;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final String viewsDir;

    public List<ColumnMetadata> getColumns(ResurfaceTableHandle tableHandle) {
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    public List<File> getFiles(SchemaTableName table, int slab) {
        return location.files().stream()
                .filter(f -> !f.isHidden() && (f.getName().endsWith(".blk") || f.getName().endsWith(".blkc")))
                .filter(f -> f.getName().startsWith(table.getTableName() + "." + slab))
                .collect(Collectors.toList());
    }

    public int getSlabs() {
        return slabs;
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
                new ColumnMetadata("bitmap_unused5", BIGINT)                                       // 48 (v3.1)
        );

        public static final String TABLE_NAME = "message";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(ResurfaceMetadata.SCHEMA_DATA, TABLE_NAME);
        }

    }

}
