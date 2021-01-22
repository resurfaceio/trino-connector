// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.resurface.binfiles.BinaryHttpMessage;
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
                return message.size_request;
            case 22:
                return message.size_response;
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
                return Slices.wrappedBuffer(message.id);
            case 1:
                return Slices.wrappedBuffer(message.agent_category);
            case 2:
                return Slices.wrappedBuffer(message.agent_device);
            case 3:
                return Slices.wrappedBuffer(message.agent_name);
            case 4:
                return Slices.wrappedBuffer(message.host);
            case 5:
                return Slices.wrappedBuffer(message.interval_category);
            case 6:
                return Slices.wrappedBuffer(message.interval_clique);
            case 8:
                return Slices.wrappedBuffer(message.request_body);
            case 9:
                return Slices.wrappedBuffer(message.request_content_type);
            case 10:
                return Slices.wrappedBuffer(message.request_headers);
            case 11:
                return Slices.wrappedBuffer(message.request_method);
            case 12:
                return Slices.wrappedBuffer(message.request_params);
            case 13:
                return Slices.wrappedBuffer(message.request_url);
            case 14:
                return Slices.wrappedBuffer(message.request_user_agent);
            case 15:
                return Slices.wrappedBuffer(message.response_body);
            case 16:
                return Slices.wrappedBuffer(message.response_code);
            case 17:
                return Slices.wrappedBuffer(message.response_content_type);
            case 18:
                return Slices.wrappedBuffer(message.response_headers);
            case 20:
                return Slices.wrappedBuffer(message.size_category);
            default:
                throw new IllegalArgumentException("Cannot get as string: " + columns.get(field).getColumnName());
        }
    }

    @Override
    public Type getType(int field) {
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean isNull(int field) {
        switch (columns.get(field).getOrdinalPosition()) {
            case 0:
                return message.id == null;
            case 1:
                return message.agent_category == null;
            case 2:
                return message.agent_device == null;
            case 3:
                return message.agent_name == null;
            case 4:
                return message.host == null;
            case 5:
                return message.interval_category == null;
            case 6:
                return message.interval_clique == null;
            case 7:
                return message.interval_millis == 0;
            case 8:
                return message.request_body == null;
            case 9:
                return message.request_content_type == null;
            case 10:
                return message.request_headers == null;
            case 11:
                return message.request_method == null;
            case 12:
                return message.request_params == null;
            case 13:
                return message.request_url == null;
            case 14:
                return message.request_user_agent == null;
            case 15:
                return message.response_body == null;
            case 16:
                return message.response_code == null;
            case 17:
                return message.response_content_type == null;
            case 18:
                return message.response_headers == null;
            case 19:
                return message.response_time_millis == 0;
            case 20:
                return message.size_category == null;
            case 21:
                return message.size_request == 0;
            case 22:
                return message.size_response == 0;
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
