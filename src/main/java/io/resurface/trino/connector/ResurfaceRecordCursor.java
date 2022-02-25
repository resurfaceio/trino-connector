// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.resurface.binfiles.BinaryHttpMessage;
import io.resurface.binfiles.BinaryHttpMessageString;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import java.io.*;
import java.util.Iterator;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

public class ResurfaceRecordCursor implements RecordCursor {

    public ResurfaceRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, SchemaTableName tableName, int slab) {
        try {
            this.column_names = new String[columns.size()];
            this.column_ordinal_positions = new int[columns.size()];
            this.column_types = new Type[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                this.column_names[i] = columns.get(i).getColumnName();
                this.column_ordinal_positions[i] = columns.get(i).getOrdinalPosition();
                this.column_types[i] = columns.get(i).getColumnType();
            }
            this.iterator = new FilesIterator(tables.getFiles(tableName, slab).iterator());
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
            case 5: // v3
                return message.graphql_operations_count.value();
            case 7:
                return message.interval_millis.value();
            case 21:
                return message.response_time_millis.value();
            case 22:
                return message.size_request_bytes.value();
            case 23:
                return message.size_response_bytes.value();
            case 28: // v3
                return message.cookies_count.value();
            case 30: // v3.1
                return message.size_request_bytes.value() + message.size_response_bytes.value();
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
            case 4: // v3
                return getSliceFromField(message.graphql_operations);
            case 6:
                return getSliceFromField(message.host);
            case 8:
                return getSliceFromField(message.request_body);
            case 9:
                return getSliceFromField(message.request_content_type);
            case 10:
                return getSliceFromField(message.request_headers);
            case 11:
                return getSliceFromField(message.request_json_type);
            case 12:
                return getSliceFromField(message.request_method);
            case 13:
                return getSliceFromField(message.request_params);
            case 14:
                return getSliceFromField(message.request_url);
            case 15:
                return getSliceFromField(message.request_user_agent);
            case 16:
                return getSliceFromField(message.response_body);
            case 17:
                return getSliceFromField(message.response_code);
            case 18:
                return getSliceFromField(message.response_content_type);
            case 19:
                return getSliceFromField(message.response_headers);
            case 20:
                return getSliceFromField(message.response_json_type);
            case 24: // v3
                return getSliceFromField(message.custom_fields);
            case 25: // v3
                return getSliceFromField(message.request_address);
            case 26: // v3
                return getSliceFromField(message.session_fields);
            case 27: // v3
                return getSliceFromField(message.cookies);
            case 29: // v3.1
                Slice type = getSliceFromField(message.response_json_type);
                if (MALFORMED.equals(type)) return MALFORMED;
                Slice body = getSliceFromField(message.response_body);
                if (body.indexOf(FIND_EXCEPTION) >= 0) return PAYLOAD_ERROR;
                if ((ARRAY.equals(type) || OBJECT.equals(type)) && (body.indexOf(FIND_ERROR)) >= 0) return PAYLOAD_ERROR;
                try {
                    int code = Integer.parseInt(getSliceFromField(message.response_code).toStringUtf8());
                    if (code == 401) return UNAUTHORIZED;
                    else if (code == 403) return FORBIDDEN;
                    else if (code == 429) return THROTTLED;
                    else if (code == 400 || code == 402 || code > 403) return HTTP_ERROR;
                } catch (NumberFormatException nfe) {
                    // do nothing
                }
                return COMPLETED;
            default:
                throw new IllegalArgumentException("Cannot get as string: " + column_names[field]);
        }
    }

    private static final Slice FIND_ERROR = utf8Slice("\"errors\":");
    private static final Slice FIND_EXCEPTION = utf8Slice("Exception:");

    private static final Slice ARRAY = utf8Slice("ARRAY");
    private static final Slice COMPLETED = utf8Slice("COMPLETED");
    private static final Slice FORBIDDEN = utf8Slice("FORBIDDEN");
    private static final Slice HTTP_ERROR = utf8Slice("HTTP_ERROR");
    private static final Slice MALFORMED = utf8Slice("MALFORMED");
    private static final Slice OBJECT = utf8Slice("OBJECT");
    private static final Slice PAYLOAD_ERROR = utf8Slice("PAYLOAD_ERROR");
    private static final Slice THROTTLED = utf8Slice("THROTTLED");
    private static final Slice UNAUTHORIZED = utf8Slice("UNAUTHORIZED");

    private Slice getSliceFromField(BinaryHttpMessageString field) {
        int len = field.length();
        return len == 0 ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(field.buffer(), field.offset(), len);
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
            case 4: // v3
                return message.graphql_operations.length() == 0;
            case 5: // v3
                return false;  // graphql_operations_count
            case 6:
                return message.host.length() == 0;
            case 7:
                return message.interval_millis.value() == 0;
            case 8:
                return message.request_body.length() == 0;
            case 9:
                return message.request_content_type.length() == 0;
            case 10:
                return message.request_headers.length() == 0;
            case 11:
                return message.request_json_type.length() == 0;
            case 12:
                return message.request_method.length() == 0;
            case 13:
                return message.request_params.length() == 0;
            case 14:
                return message.request_url.length() == 0;
            case 15:
                return message.request_user_agent.length() == 0;
            case 16:
                return message.response_body.length() == 0;
            case 17:
                return message.response_code.length() == 0;
            case 18:
                return message.response_content_type.length() == 0;
            case 19:
                return message.response_headers.length() == 0;
            case 20:
                return message.response_json_type.length() == 0;
            case 21:
                return message.response_time_millis.value() == 0;
            case 22:
                return message.size_request_bytes.value() == 0;
            case 23:
                return message.size_response_bytes.value() == 0;
            case 24: // v3
                return message.custom_fields.length() == 0;
            case 25: // v3
                return message.request_address.length() == 0;
            case 26: // v3
                return message.session_fields.length() == 0;
            case 27: // v3
                return message.cookies.length() == 0;
            case 28: // v3
                return false;  // cookies_count
            case 29: // v3.1
                return false;  // response_status
            case 30: // v3.1
                return false;  // size_total_bytes
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
                } catch (EOFException | RuntimeException | StreamCorruptedException e) {
                    stream.close();
                    stream = createNextStream();
                }
            }
            message = null;
        }

        private FastBufferedInputStream createNextStream() {
            if (!files.hasNext()) return null;
            File file = files.next();
            try {
                FileInputStream fis = new FileInputStream(file);
                return new FastBufferedInputStream(fis, 1000000);
            } catch (FileNotFoundException e) {
                return createNextStream();
            }
        }

        private final Iterator<File> files;
        private FastBufferedInputStream stream;

    }

}
