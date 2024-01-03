// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.io.*;
import java.util.Iterator;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

public class ShardRecordCursor implements RecordCursor {

    public ShardRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, ResurfaceTableHandle handle, String node_id, int slab) {
        this.column_names = new String[columns.size()];
        this.column_ordinal_positions = new int[columns.size()];
        this.column_types = new Type[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            this.column_names[i] = columns.get(i).getColumnName();
            this.column_ordinal_positions[i] = columns.get(i).getOrdinalPosition();
            this.column_types[i] = columns.get(i).getColumnType();
        }
        this.node_id = node_id;
        this.files = tables.getFiles(handle, slab).iterator();
        this.open_file = tables.getOpenFile();
    }

    private final String[] column_names;
    private final int[] column_ordinal_positions;
    private final Type[] column_types;
    private final Iterator<File> files;
    private final String node_id;
    private final String open_file;
    private File shard_file;

    @Override
    public boolean advanceNextPosition() {
        if (files.hasNext()) {
            shard_file = files.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean getBoolean(int field) {
        return switch (column_ordinal_positions[field]) {
            case 4 -> shard_file.getName().equals(open_file);
            default -> throw new IllegalArgumentException("Cannot get as long: " + column_names[field]);
        };
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public double getDouble(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field) {
        return switch (column_ordinal_positions[field]) {
            case 2 -> shard_file.lastModified();
            case 3 -> shard_file.length();
            default -> throw new IllegalArgumentException("Cannot get as long: " + column_names[field]);
        };
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Slice getSlice(int field) {
        return switch (column_ordinal_positions[field]) {
            case 0 -> utf8Slice(node_id);
            case 1 -> utf8Slice(shard_file.getName());
            default -> throw new IllegalArgumentException("Cannot get as string: " + column_names[field]);
        };
    }

    @Override
    public Type getType(int field) {
        return column_types[field];
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

}
