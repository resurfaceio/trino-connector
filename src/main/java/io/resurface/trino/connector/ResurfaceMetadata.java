// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResurfaceMetadata implements ConnectorMetadata {

    public static final String SCHEMA_NAME = "data";

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(SCHEMA_NAME);

    @Inject
    public ResurfaceMetadata(ResurfaceTables tables) {
        this.tables = requireNonNull(tables, "tables is null");
    }

    private final ResurfaceTables tables;

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                                                                                   ConnectorTableHandle table,
                                                                                   Constraint constraint) {
        ResurfaceTableHandle handle = (ResurfaceTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) return Optional.empty();
        handle = new ResurfaceTableHandle(handle.getSchemaTableName(), newDomain);
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ResurfaceTableHandle tableHandle = (ResurfaceTableHandle) table;
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(ResurfaceTableHandle tableHandle) {
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : tables.getColumns(tableHandle)) {
            int ordinalPosition = index++;
            handles.put(column.getName(), new ResurfaceColumnHandle(column.getName(), column.getType(), ordinalPosition));
        }
        return handles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((ResurfaceColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        requireNonNull(tableName, "tableName is null");
        return tables.getTable(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ResurfaceTableHandle tableHandle = (ResurfaceTableHandle) table;
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), tables.getColumns(tableHandle));
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
            ResurfaceTableHandle tableHandle = tables.getTable(tableName);
            if (tableHandle != null) columns.put(tableName, tables.getColumns(tableHandle));
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return tables.getTables();
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
