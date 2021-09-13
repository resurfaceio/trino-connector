// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.resurface.trino.connector.ResurfaceTables.MessageTable.TABLE_NAME;
import static io.trino.spi.StandardErrorCode.*;
import static java.util.Objects.requireNonNull;

public class ResurfaceMetadata implements ConnectorMetadata {

    public static final String SCHEMA_DATA = "data";

    public static final String SCHEMA_RUNTIME = "runtime";

    public static final String SCHEMA_SETTINGS = "settings";

    public static final String SCHEMA_VOLATILE = "volatile";

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(SCHEMA_DATA, SCHEMA_RUNTIME, SCHEMA_SETTINGS, SCHEMA_VOLATILE);

    @Inject
    public ResurfaceMetadata(ResurfaceTables tables) {
        this.tables = requireNonNull(tables, "tables is null");
        if (tables.getViewsDir() != null) buildViews();
    }

    private final ResurfaceTables tables;
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

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
        HashMap<SchemaTableName, List<ColumnMetadata>> result = new HashMap<>();
        result.put(new SchemaTableName(SCHEMA_DATA, TABLE_NAME), ResurfaceTables.MessageTable.COLUMNS);
        return result;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        builder.add(new SchemaTableName(SCHEMA_DATA, TABLE_NAME));
        views.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .forEach(builder::add);
        return builder.build();
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        return false;
    }

    // VIEWS, BABY, VIEWS --------------------------------------------------------------------------------------------------------

    private synchronized void buildViews() {
        File dir = new File(tables.getViewsDir());
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new TrinoException(CONFIGURATION_INVALID, "Unable to access directory: " + tables.getViewsDir());

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());

        DataLocation loc = new DataLocation(tables.getViewsDir());
        List<File> files = loc.files().stream()
                .filter(f -> !f.isHidden() && f.getName().endsWith(".json"))
                .collect(Collectors.toList());

        for (File f : files) {
            try {
                ConnectorViewDefinition def = mapper.readValue(f, ConnectorViewDefinition.class);
                String filename = f.getName();
                String[] name_pieces = filename.split("\\.");
                if ((name_pieces.length == 3) && SCHEMA_NAMES.contains(name_pieces[0]) && name_pieces[2].equals("json"))
                    views.put(new SchemaTableName(name_pieces[0], name_pieces[1]), def);
            } catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read file: " + f);
            }
        }
    }

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace) {
        if (tables.getViewsDir() == null)
            throw new TrinoException(CONFIGURATION_INVALID, "Not configured for persistent views");

        String schema = viewName.getSchemaName();
        if (!SCHEMA_NAMES.contains(schema)) {
            throw new TrinoException(READ_ONLY_VIOLATION, "Schema is read only: " + viewName);
        } else if (replace) {
            views.put(viewName, definition);
        } else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }

        if (schema.equals(SCHEMA_VOLATILE)) return;

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        try {
            String json = mapper.writeValueAsString(definition);
            File f = new File(new File(tables.getViewsDir()), schema + "." + viewName.getTableName() + ".json");
            Files.write(Paths.get(f.toURI()), json.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName) {
        if (tables.getViewsDir() == null)
            throw new TrinoException(CONFIGURATION_INVALID, "Not configured for persistent views");

        String schema = viewName.getSchemaName();
        if (!SCHEMA_NAMES.contains(schema)) {
            throw new TrinoException(READ_ONLY_VIOLATION, "Schema is read only: " + viewName);
        } else if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }

        if (schema.equals(SCHEMA_VOLATILE)) return;

        try {
            File f = new File(new File(tables.getViewsDir()), schema + "." + viewName.getTableName() + ".json");
            Files.deleteIfExists(Paths.get(f.toURI()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName) {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(Maps.filterKeys(views, prefix::matches));
    }

    @Override
    public synchronized Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName) {
        return Optional.ofNullable(views.get(viewName));
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName) {
        return views.keySet().stream()
                .filter(viewName -> schemaName.map(viewName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

}
