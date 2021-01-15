// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.resurface.trino.connector.ResurfaceTables.HttpRequestLogTable.getSchemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.concurrent.TimeUnit.SECONDS;

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
            tableColumnsBuilder.put(table, HttpRequestLogTable.getColumns());
            dataLocationBuilder.put(table, dataLocation);
        }

        tables = tablesBuilder.build();
        tableColumns = tableColumnsBuilder.build();
        tableDataLocations = dataLocationBuilder.build();

        cachedFiles = CacheBuilder.newBuilder()
                .expireAfterWrite(10, SECONDS)
                .build(CacheLoader.from(key -> tableDataLocations.get(key).files()));
    }

    private final LoadingCache<SchemaTableName, List<File>> cachedFiles;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final Map<SchemaTableName, DataLocation> tableDataLocations;
    private final Map<SchemaTableName, ResurfaceTableHandle> tables;

    public List<ColumnMetadata> getColumns(ResurfaceTableHandle tableHandle) {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table '%s' not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    public List<File> getFiles(SchemaTableName table) {
        try {
            return cachedFiles.getUnchecked(table);
        } catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    public ResurfaceTableHandle getTable(SchemaTableName tableName) {
        return tables.get(tableName);
    }

    public List<SchemaTableName> getTables() {
        return ImmutableList.copyOf(tables.keySet());
    }

    public static final class HttpRequestLogTable {

        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                new ColumnMetadata("timestamp", createTimestampWithTimeZoneType(3)),
                new ColumnMetadata("client_address", createUnboundedVarcharType()),
                new ColumnMetadata("method", createUnboundedVarcharType()),
                new ColumnMetadata("request_uri", createUnboundedVarcharType()),
                new ColumnMetadata("user", createUnboundedVarcharType()),
                new ColumnMetadata("agent", createUnboundedVarcharType()),
                new ColumnMetadata("response_code", BIGINT),
                new ColumnMetadata("request_size", BIGINT),
                new ColumnMetadata("response_size", BIGINT),
                new ColumnMetadata("time_to_last_byte", BIGINT),
                new ColumnMetadata("trace_token", createUnboundedVarcharType()));

        private static final String TABLE_NAME = "message";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(ResurfaceMetadata.SCHEMA_NAME, TABLE_NAME);
        }

    }

}
