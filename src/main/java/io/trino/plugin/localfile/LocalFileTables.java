// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

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
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.localfile.LocalFileMetadata.PRESTO_LOGS_SCHEMA;
import static io.trino.plugin.localfile.LocalFileMetadata.SERVER_ADDRESS_COLUMN;
import static io.trino.plugin.localfile.LocalFileTables.HttpRequestLogTable.*;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LocalFileTables {

    @Inject
    public LocalFileTables(LocalFileConfig config) {
        ImmutableMap.Builder<SchemaTableName, DataLocation> dataLocationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, LocalFileTableHandle> tablesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        String httpRequestLogLocation = config.getHttpRequestLogLocation();
        if (httpRequestLogLocation != null) {
            Optional<String> pattern = Optional.empty();
            if (config.getHttpRequestLogFileNamePattern() != null) {
                pattern = Optional.of(config.getHttpRequestLogFileNamePattern());
            }

            SchemaTableName table = getSchemaTableName();
            DataLocation dataLocation = new DataLocation(httpRequestLogLocation, pattern);
            LocalFileTableHandle tableHandle = new LocalFileTableHandle(table, getTimestampColumn(), getServerAddressColumn());

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
    private final Map<SchemaTableName, LocalFileTableHandle> tables;

    public List<ColumnMetadata> getColumns(LocalFileTableHandle tableHandle) {
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

    public LocalFileTableHandle getTable(SchemaTableName tableName) {
        return tables.get(tableName);
    }

    public List<SchemaTableName> getTables() {
        return ImmutableList.copyOf(tables.keySet());
    }

    public static final class HttpRequestLogTable {

        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                SERVER_ADDRESS_COLUMN,
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

        private static final String TABLE_NAME = "http_request_log";

        public static List<ColumnMetadata> getColumns() {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName() {
            return new SchemaTableName(PRESTO_LOGS_SCHEMA, TABLE_NAME);
        }

        public static OptionalInt getServerAddressColumn() {
            return OptionalInt.of(-1);
        }

        public static OptionalInt getTimestampColumn() {
            return OptionalInt.of(0);
        }

    }

}
