// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResurfaceRecordSet implements RecordSet {

    public ResurfaceRecordSet(ResurfaceTables tables, ResurfaceSplit split, ResurfaceTableHandle handle, List<ResurfaceColumnHandle> columns) {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ResurfaceColumnHandle column : columns) types.add(column.getColumnType());
        this.columnTypes = types.build();
        this.handle = handle;
        this.node_id = split.getNodeId();
        this.slab = split.getSlab();
        this.tables = requireNonNull(tables, "tables is null");
    }

    private final List<ResurfaceColumnHandle> columns;
    private final List<Type> columnTypes;
    private final ResurfaceTableHandle handle;
    private final String node_id;
    private final int slab;
    private final ResurfaceTables tables;

    @Override
    public RecordCursor cursor() {
        String table = handle.getSchemaTableName().getTableName();
        if (ResurfaceTables.MessageTable.TABLE_NAME.equals(table)) {
            return new MessageRecordCursor(tables, columns, handle, slab);
        } else if (ResurfaceTables.ShardTable.TABLE_NAME.equals(table)) {
            return new ShardRecordCursor(tables, columns, handle, node_id, slab);
        }
        throw new IllegalArgumentException("Table not implemented");
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

}
