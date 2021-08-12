// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static java.util.Objects.requireNonNull;

public class ResurfaceMetadata implements ConnectorMetadata {

    public static final String SCHEMA_DATA = "data";

    public static final String SCHEMA_RUNTIME = "runtime";

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(SCHEMA_DATA, SCHEMA_RUNTIME);

    @Inject
    public ResurfaceMetadata(ResurfaceTables tables) {
        this.tables = requireNonNull(tables, "tables is null");
    }

    private final ResurfaceTables tables;
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                                                                                   ConnectorTableHandle table,
                                                                                   Constraint constraint) {
        ResurfaceTableHandle handle = (ResurfaceTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) return Optional.empty();
        handle = new ResurfaceTableHandle(handle.getSchemaTableName(), newDomain);
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), false));
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
        System.out.println("!!!!! [ResurfaceMetadata] listTables");
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        builder.add(new SchemaTableName(SCHEMA_DATA, "message"));
        views.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .forEach(builder::add);
        return builder.build();
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

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace) {
        System.out.println("!!!!! [ResurfaceMetadata] createView");
        if (!viewName.getSchemaName().equals(SCHEMA_RUNTIME)) {
            throw new TrinoException(READ_ONLY_VIOLATION, "Schema is ready only: " + viewName);
        } else if (replace) {
            views.put(viewName, definition);
        } else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName) {
        System.out.println("!!!!! [ResurfaceMetadata] dropView");
        if (!viewName.getSchemaName().equals(SCHEMA_RUNTIME)) {
            throw new TrinoException(READ_ONLY_VIOLATION, "Schema is ready only: " + viewName);
        } else if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName) {
        System.out.println("!!!!! [ResurfaceMetadata] getViews");
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(Maps.filterKeys(views, prefix::matches));
    }

    @Override
    public synchronized Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName) {
        System.out.println("!!!!! [ResurfaceMetadata] getView");
        return Optional.ofNullable(views.get(viewName));
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName) {
        System.out.println("!!!!! [ResurfaceMetadata] listViews");
        return views.keySet().stream()
                .filter(viewName -> schemaName.map(viewName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName) {
        System.out.println("!!!!! [ResurfaceMetadata] renameView");
        if (!viewName.getSchemaName().equals(SCHEMA_RUNTIME)) {
            throw new TrinoException(READ_ONLY_VIOLATION, "Schema is ready only: " + viewName);
        } else if (views.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + newViewName);
        }
        views.put(newViewName, views.remove(viewName));
    }

}
