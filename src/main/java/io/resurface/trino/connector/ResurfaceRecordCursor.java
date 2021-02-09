// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.resurface.binfiles.BinaryHttpMessage;
import io.resurface.binfiles.BinaryHttpMessageString;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class ResurfaceRecordCursor implements RecordCursor {

    public ResurfaceRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, SchemaTableName tableName) {
        try {
            this.column_names = new String[columns.size()];
            this.column_ordinal_positions = new int[columns.size()];
            this.column_types = new Type[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                this.column_names[i] = columns.get(i).getColumnName();
                this.column_ordinal_positions[i] = columns.get(i).getOrdinalPosition();
                this.column_types[i] = columns.get(i).getColumnType();
            }
            this.iterator = new FilesIterator(tables.getFiles(tableName).iterator());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final String[] column_names;
    private final int[] column_ordinal_positions;
    private final Type[] column_types;
    private final FilesIterator iterator;
    private BinaryHttpMessage message = new BinaryHttpMessage();

    @Override
    public boolean advanceNextPosition() {
        try {
            iterator.readMessage();
            return message != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        iterator.close();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field) {
        switch (column_ordinal_positions[field]) {
            case 7:
                return message.interval_millis.value();
            case 19:
                return message.response_time_millis.value();
            case 21:
                return message.size_request_bytes.value();
            case 22:
                return message.size_response_bytes.value();
            default:
                throw new IllegalArgumentException("Cannot get as long: " + column_names[field]);
        }
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
        switch (column_ordinal_positions[field]) {
            case 0:
                return getSliceFromField(message.id);
            case 1:
                return getSliceFromField(message.agent_category);
            case 2:
                return getSliceFromField(message.agent_device);
            case 3:
                return getSliceFromField(message.agent_name);
            case 4:
                return getSliceFromField(message.host);
            case 5:
                return getSliceFromField(message.interval_category);
            case 6:
                return getSliceFromField(message.interval_clique);
            case 8:
                return getSliceFromField(message.request_body);
            case 9:
                return getSliceFromField(message.request_content_type);
            case 10:
                return getSliceFromField(message.request_headers);
            case 11:
                return getSliceFromField(message.request_method);
            case 12:
                return getSliceFromField(message.request_params);
            case 13:
                return getSliceFromField(message.request_url);
            case 14:
                return getSliceFromField(message.request_user_agent);
            case 15:
                return getSliceFromField(message.response_body);
            case 16:
                return getSliceFromField(message.response_code);
            case 17:
                return getSliceFromField(message.response_content_type);
            case 18:
                return getSliceFromField(message.response_headers);
            case 20:
                return getSliceFromField(message.size_category);
            default:
                throw new IllegalArgumentException("Cannot get as string: " + column_names[field]);
        }
    }

    private Slice getSliceFromField(BinaryHttpMessageString field) {
        int len = field.length();
        return len == 0 ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(field.buffer(), 0, len);
    }

    @Override
    public Type getType(int field) {
        return column_types[field];
    }

    @Override
    public boolean isNull(int field) {
        switch (column_ordinal_positions[field]) {
            case 0:
                return message.id.length() == 0;
            case 1:
                return message.agent_category.length() == 0;
            case 2:
                return message.agent_device.length() == 0;
            case 3:
                return message.agent_name.length() == 0;
            case 4:
                return message.host.length() == 0;
            case 5:
                return message.interval_category.length() == 0;
            case 6:
                return message.interval_clique.length() == 0;
            case 7:
                return message.interval_millis.value() == 0;
            case 8:
                return message.request_body.length() == 0;
            case 9:
                return message.request_content_type.length() == 0;
            case 10:
                return message.request_headers.length() == 0;
            case 11:
                return message.request_method.length() == 0;
            case 12:
                return message.request_params.length() == 0;
            case 13:
                return message.request_url.length() == 0;
            case 14:
                return message.request_user_agent.length() == 0;
            case 15:
                return message.response_body.length() == 0;
            case 16:
                return message.response_code.length() == 0;
            case 17:
                return message.response_content_type.length() == 0;
            case 18:
                return message.response_headers.length() == 0;
            case 19:
                return message.response_time_millis.value() == 0;
            case 20:
                return message.size_category.length() == 0;
            case 21:
                return message.size_request_bytes.value() == 0;
            case 22:
                return message.size_response_bytes.value() == 0;
            default:
                throw new IllegalArgumentException("Invalid field index: " + field);
        }
    }

    private class FilesIterator {

        public FilesIterator(Iterator<File> files) throws IOException {
            this.files = files;
            this.stream = createNextStream();
        }

        public void close() {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException ignored) {
                    // nothing to do here
                }
            }
        }

        public void readMessage() throws IOException {
            while (stream != null) {
                try {
                    message.read(stream);
                    return;
                } catch (EOFException eof) {
                    stream.close();
                    stream = createNextStream();
                }
            }
            message = null;
        }

        private ObjectInputStream createNextStream() throws IOException {
            if (!files.hasNext()) return null;
            File file = files.next();
            try {
                FileInputStream fis = new FileInputStream(file);
                BufferedInputStream bis = new BufferedInputStream(fis, 1000000);
                return new ObjectInputStream(bis);
            } catch (EOFException | FileNotFoundException e) {
                return createNextStream();
            }
        }

        private final Iterator<File> files;
        private ObjectInputStream stream;

    }

}