// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.localfile.LocalFileColumnHandle.SERVER_ADDRESS_COLUMN_NAME;
import static io.trino.plugin.localfile.LocalFileColumnHandle.SERVER_ADDRESS_ORDINAL_POSITION;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class LocalFileMetadata implements ConnectorMetadata {

    public static final String PRESTO_LOGS_SCHEMA = "logs";
    public static final ColumnMetadata SERVER_ADDRESS_COLUMN = new ColumnMetadata("server_address", createUnboundedVarcharType());
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_LOGS_SCHEMA);

    @Inject
    public LocalFileMetadata(LocalFileTables localFileTables) {
        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    private final LocalFileTables localFileTables;

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                                                                                   ConnectorTableHandle table,
                                                                                   Constraint constraint) {
        LocalFileTableHandle handle = (LocalFileTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) return Optional.empty();

        handle = new LocalFileTableHandle(
                handle.getSchemaTableName(),
                handle.getTimestampColumn(),
                handle.getServerAddressColumn(),
                newDomain);

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        LocalFileTableHandle tableHandle = (LocalFileTableHandle) table;
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(LocalFileTableHandle tableHandle) {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : localFileTables.getColumns(tableHandle)) {
            int ordinalPosition;
            if (column.getName().equals(SERVER_ADDRESS_COLUMN_NAME)) {
                ordinalPosition = SERVER_ADDRESS_ORDINAL_POSITION;
            } else {
                ordinalPosition = index;
                index++;
            }
            columnHandles.put(column.getName(), new LocalFileColumnHandle(column.getName(), column.getType(), ordinalPosition));
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((LocalFileColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        requireNonNull(tableName, "tableName is null");
        return localFileTables.getTable(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        LocalFileTableHandle tableHandle = (LocalFileTableHandle) table;
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), localFileTables.getColumns(tableHandle));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return new ConnectorTableProperties();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return SCHEMA_NAMES;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            LocalFileTableHandle tableHandle = localFileTables.getTable(tableName);
            if (tableHandle != null) columns.put(tableName, localFileTables.getColumns(tableHandle));
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return localFileTables.getTables();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        } else {
            return ImmutableList.of(prefix.toSchemaTableName());
        }
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        return false;
    }

}
