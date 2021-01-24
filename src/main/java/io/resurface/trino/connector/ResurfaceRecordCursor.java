// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.resurface.binfiles.BinaryHttpMessage;
import io.resurface.binfiles.BinaryHttpMessageField;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class ResurfaceRecordCursor implements RecordCursor {

    public ResurfaceRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, SchemaTableName tableName) {
        try {
            this.columns = columns;
            this.iterator = new FilesIterator(tables.getFiles(tableName).iterator());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final List<ResurfaceColumnHandle> columns;
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
        switch (columns.get(field).getOrdinalPosition()) {
            case 7:
                return message.interval_millis;
            case 19:
                return message.response_time_millis;
            case 21:
                return message.size_request_bytes;
            case 22:
                return message.size_response_bytes;
            default:
                throw new IllegalArgumentException("Cannot get as long: " + columns.get(field).getColumnName());
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
        switch (columns.get(field).getOrdinalPosition()) {
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
                throw new IllegalArgumentException("Cannot get as string: " + columns.get(field).getColumnName());
        }
    }

    private Slice getSliceFromField(BinaryHttpMessageField field) {
        return field.len == 0 ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(field.buffer, 0, field.len);
    }

    @Override
    public Type getType(int field) {
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean isNull(int field) {
        switch (columns.get(field).getOrdinalPosition()) {
            case 0:
                return message.id.len == 0;
            case 1:
                return message.agent_category.len == 0;
            case 2:
                return message.agent_device.len == 0;
            case 3:
                return message.agent_name.len == 0;
            case 4:
                return message.host.len == 0;
            case 5:
                return message.interval_category.len == 0;
            case 6:
                return message.interval_clique.len == 0;
            case 7:
                return message.interval_millis == 0;
            case 8:
                return message.request_body.len == 0;
            case 9:
                return message.request_content_type.len == 0;
            case 10:
                return message.request_headers.len == 0;
            case 11:
                return message.request_method.len == 0;
            case 12:
                return message.request_params.len == 0;
            case 13:
                return message.request_url.len == 0;
            case 14:
                return message.request_user_agent.len == 0;
            case 15:
                return message.response_body.len == 0;
            case 16:
                return message.response_code.len == 0;
            case 17:
                return message.response_content_type.len == 0;
            case 18:
                return message.response_headers.len == 0;
            case 19:
                return message.response_time_millis == 0;
            case 20:
                return message.size_category.len == 0;
            case 21:
                return message.size_request_bytes == 0;
            case 22:
                return message.size_response_bytes == 0;
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
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis, 1000000);
            try {
                return new ObjectInputStream(bis);
            } catch (EOFException eof) {
                return createNextStream();
            }
        }

        private final Iterator<File> files;
        private ObjectInputStream stream;

    }

}
