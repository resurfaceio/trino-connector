// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResurfaceRecordCursor implements RecordCursor {

    public ResurfaceRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, SchemaTableName tableName) {
        this.columns = requireNonNull(columns, "columns is null");

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ResurfaceColumnHandle handle = columns.get(i);
            fieldToColumnIndex[i] = handle.getOrdinalPosition();
        }

        try {
            this.reader = new FilesReader(tables.getFiles(tableName).iterator());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final List<ResurfaceColumnHandle> columns;
    private final int[] fieldToColumnIndex;
    private final FilesReader reader;
    private List<String> fields;

    @Override
    public boolean advanceNextPosition() {
        try {
            fields = reader.readFields();
            return fields != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public boolean getBoolean(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT, INTEGER);
        return Long.parseLong(getFieldValue(field));
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
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columns.size(), "Invalid field index");
        String fieldValue = getFieldValue(field);
        return "null".equals(fieldValue) || Strings.isNullOrEmpty(fieldValue);
    }

    private void checkFieldType(int field, Type... expected) {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) return;
        }
        String expectedTypes = Joiner.on(", ").join(expected);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", field, expectedTypes, actual));
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");
        int index = fieldToColumnIndex[field];
        if (index >= fields.size()) return null;
        return fields.get(index);
    }

    private static class FilesReader {

        public FilesReader(Iterator<File> files) throws IOException {
            requireNonNull(files, "files is null");
            this.files = files;
            reader = createNextReader();
        }

        private final Iterator<File> files;
        private BufferedReader reader;

        public void close() {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }

        private BufferedReader createNextReader() throws IOException {
            if (!files.hasNext()) return null;
            File file = files.next();
            FileInputStream fileInputStream = new FileInputStream(file);
            return new BufferedReader(new InputStreamReader(fileInputStream));
        }

        public List<String> readFields() throws IOException {
            List<String> fields = null;
            boolean newReader = false;

            while (fields == null) {
                if (reader == null) return null;
                String line = reader.readLine();
                if (line != null) {
                    fields = new ArrayList<>();
                    fields.add(null);  // timestamp
                    fields.add("192.168.4.61");
                    fields.add("POST");
                    fields.add("/v1/memory");
                    fields.add("rdickinson");
                    fields.add("myagent");
                    fields.add("200");
                    fields.add("73");
                    fields.add("269");
                    fields.add("2");
                    fields.add("asdf1234");
                    if (!newReader) return fields;
                }
                reader.close();
                reader = createNextReader();
                newReader = true;
            }

            return fields;
        }

    }

}
