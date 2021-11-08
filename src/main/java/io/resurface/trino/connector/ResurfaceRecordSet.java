// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResurfaceRecordSet implements RecordSet {

    public ResurfaceRecordSet(ResurfaceTables tables, ResurfaceSplit split,
                              ResurfaceTableHandle table, List<ResurfaceColumnHandle> columns) {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ResurfaceColumnHandle column : columns) types.add(column.getColumnType());
        this.columnTypes = types.build();
        this.slab = split.getSlab();
        this.tableName = table.getSchemaTableName();
        this.tables = requireNonNull(tables, "tables is null");
    }

    private final List<ResurfaceColumnHandle> columns;
    private final List<Type> columnTypes;
    private final int slab;
    private final ResurfaceTables tables;
    private final SchemaTableName tableName;

    @Override
    public RecordCursor cursor() {
        return new ResurfaceRecordCursor(tables, columns, tableName, slab);
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

}
