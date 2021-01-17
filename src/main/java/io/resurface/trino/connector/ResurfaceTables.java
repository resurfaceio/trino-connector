// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.resurface.trino.connector.ResurfaceTables.MessageTable.getSchemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class ResurfaceTables {

    @Inject
    public ResurfaceTables(ResurfaceConfig config) {
        ImmutableMap.Builder<SchemaTableName, DataLocation> dataLocationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, ResurfaceTableHandle> tablesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        String httpRequestLogLocation = config.getHttpRequestLogLocation();
        if (httpRequestLogLocation != null) {
            Optional<String> pattern = Optional.empty();
            if (config.getHttpRequestLogFileNamePattern() != null) {
                pattern = Optional.of(config.getHttpRequestLogFileNamePattern());
            }

            SchemaTableName table = getSchemaTableName();
            DataLocation dataLocation = new DataLocation(httpRequestLogLocation, pattern);
            ResurfaceTableHandle tableHandle = new ResurfaceTableHandle(table);

            tablesBuilder.put(table, tableHandle);
            tableColumnsBuilder.put(table, MessageTable.getColumns());
            dataLocationBuilder.put(table, dataLocation);
        }

        tables = tablesBuilder.build();
        tableColumns = tableColumnsBuilder.build();
        tableDataLocations = dataLocationBuilder.build();
    }

    public List<ColumnMetadata> getColumns(ResurfaceTableHandle tableHandle) {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table '%s' not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    public List<File> getFiles(SchemaTableName table) {
        List<File> result = new ArrayList<>();
        result.add(new File("/Users/robfromboulder/Downloads/flukeserver.bin"));
        return result;
    }

    public ResurfaceTableHandle getTable(SchemaTableName tableName) {
        return tables.get(tableName);
    }

    public List<SchemaTableName> getTables() {
        return ImmutableList.copyOf(tables.keySet());
    }

    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final Map<SchemaTableName, DataLocation> tableDataLocations;
    private final Map<SchemaTableName, ResurfaceTableHandle> tables;

    public static final class MessageTable {

        public static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                new ColumnMetadata("id", createUnboundedVarcharType()),                     // 0
                new ColumnMetadata("agent_category", createUnboundedVarcharType()),         // 1
                new ColumnMetadata("agent_device", createUnboundedVarcharType()),           // 2
                new ColumnMetadata("agent_name", createUnboundedVarcharType()),             // 3
                new ColumnMetadata("host", createUnboundedVarcharType()),                   // 4
                new ColumnMetadata("interval_category", createUnboundedVarcharType()),      // 5
                new ColumnMetadata("interval_clique", createUnboundedVarcharType()),        // 6
                new ColumnMetadata("interval_millis", BIGINT),                              // 7
                new ColumnMetadata("request_body", createUnboundedVarcharType()),           // 8
                new ColumnMetadata("request_content_type", createUnboundedVarcharType()),   // 9
                new ColumnMetadata("request_headers", createUnboundedVarcharType()),        // 10
                new ColumnMetadata("request_method", createUnboundedVarcharType()),         // 11
                new ColumnMetadata("request_params", createUnboundedVarcharType()),         // 12
                new ColumnMetadata("request_url", createUnboundedVarcharType()),            // 13
                new ColumnMetadata("request_user_agent", createUnboundedVarcharType()),     // 14
                new ColumnMetadata("response_body", createUnboundedVarcharType()),          // 15
                new ColumnMetadata("response_code", createUnboundedVarcharType()),          // 16
                new ColumnMetadata("response_content_type", createUnboundedVarcharType()),  // 17
                new ColumnMetadata("response_headers", createUnboundedVarcharType()),       // 18
                new ColumnMetadata("response_time_millis", BIGINT),                         // 19
                new ColumnMetadata("size_category", createUnboundedVarcharType()),          // 20
                new ColumnMetadata("size_request", INTEGER),                                // 21
                new ColumnMetadata("size_response", INTEGER)                                // 22
        );

        public static final String TABLE_NAME = "message";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(ResurfaceMetadata.SCHEMA_NAME, TABLE_NAME);
        }

    }

}
