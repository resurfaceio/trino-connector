// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.resurface.binfiles.CompressedHttpMessage;
import io.resurface.binfiles.PersistentHttpMessage;
import io.resurface.binfiles.PersistentHttpMessageString;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import java.io.*;
import java.util.Iterator;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;

public class MessageRecordCursor implements RecordCursor {

    public MessageRecordCursor(ResurfaceTables tables, List<ResurfaceColumnHandle> columns, ResurfaceTableHandle handle, int slab) {
        this.column_names = new String[columns.size()];
        this.column_ordinal_positions = new int[columns.size()];
        this.column_types = new Type[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            this.column_names[i] = columns.get(i).getColumnName();
            this.column_ordinal_positions[i] = columns.get(i).getOrdinalPosition();
            this.column_types[i] = columns.get(i).getColumnType();
        }
        this.files = tables.getFiles(handle, slab).iterator();
        this.stream = buildNextStream();
    }

    private final String[] column_names;
    private final int[] column_ordinal_positions;
    private final Type[] column_types;
    private final Iterator<File> files;
    private final Logger log = Logger.get(MessageRecordCursor.class);
    private PersistentHttpMessage message;
    private Slice shard_file;
    private FastBufferedInputStream stream;

    @Override
    public boolean advanceNextPosition() {
        try {
            while (stream != null) {
                try {
                    message.read(stream);
                    return true;
                } catch (EOFException | RuntimeException | StreamCorruptedException e) {
                    stream.close();
                    stream = buildNextStream();
                }
            }
            message = null;
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FastBufferedInputStream buildNextStream() {
        if (!files.hasNext()) {
            return null;
        } else {
            File f = files.next();
            try {
                message = new CompressedHttpMessage();
                shard_file = utf8Slice(f.getName());
                return new FastBufferedInputStream(new FileInputStream(f), 1000000);
            } catch (FileNotFoundException e) {
                return buildNextStream();
            }
        }
    }

    @Override
    public void close() {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ignored) {
                // nothing to do here
            }
        }
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
            case 31: // v3.1
                return message.bitmap_versioning.value();
            case 32: // v3.1
                return message.bitmap_request_info.value();
            case 33: // v3.1
                return message.bitmap_request_json.value();
            case 34: // v3.1
                return message.bitmap_request_graphql.value();
            case 35: // v3.1
                return message.bitmap_request_pii.value();
            case 36: // v3.1
                return message.bitmap_request_threat.value();
            case 37: // v3.1
                return message.bitmap_response_info.value();
            case 38: // v3.1
                return message.bitmap_response_json.value();
            case 39: // v3.1
                return message.bitmap_response_pii.value();
            case 40: // v3.1
                return message.bitmap_response_threat.value();
            case 41: // v3.1
                return message.bitmap_attack_request.value();
            case 42: // v3.1
                return message.bitmap_attack_application.value();
            case 43: // v3.1
                return message.bitmap_attack_injection.value();
            case 44: // v3.1
                return message.bitmap_response_leak.value();
            case 45: // v3.1
                return message.bitmap_unused2.value();
            case 46: // v3.1
                return message.bitmap_unused3.value();
            case 47: // v3.1
                return message.bitmap_unused4.value();
            case 48: // v3.1
                return message.bitmap_unused5.value();
            case 50: // v3.6
                return message.host.length();
            case 51: // v3.6
                return message.request_body.length();
            case 52: // v3.6
                return message.request_headers.length();
            case 53: // v3.6
                return message.request_params.length();
            case 54: // v3.6
                return message.request_url.length();
            case 55: // v3.6
                return message.response_body.length();
            case 56: // v3.6
                return message.response_headers.length();
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
            case 29: // v3.1 (response_status)
                return Slices.EMPTY_SLICE;
            case 49: // v3.5
                return shard_file;
            default:
                throw new IllegalArgumentException("Cannot get as string: " + column_names[field]);
        }
    }

    private Slice getSliceFromField(PersistentHttpMessageString field) {
        try {
            return field.isNull() ? Slices.EMPTY_SLICE : field.toSlice();
        } catch (Exception e) {
            log.error("getSliceFromField failed:"
                    + "\nfield.length=" + field.length()
                    + "\nfield.offset=" + field.offset()
                    + "\nmessage.id=" + message.id.value()
                    + "\nmessage.id.length=" + message.id.length()
                    + "\nmessage.agent_category.length=" + message.agent_category.length()
                    + "\nmessage.agent_device.length=" + message.agent_device.length()
                    + "\nmessage.agent_name.length=" + message.agent_name.length()
                    + "\nmessage.graphql_operations.length=" + message.graphql_operations.length()
                    + "\nmessage.host.length=" + message.host.length()
                    + "\nmessage.request_body.length=" + message.request_body.length()
                    + "\nmessage.request_content_type.length=" + message.request_content_type.length()
                    + "\nmessage.request_headers.length=" + message.request_headers.length()
                    + "\nmessage.request_json_type.length=" + message.request_json_type.length()
                    + "\nmessage.request_method.length=" + message.request_method.length()
                    + "\nmessage.request_params.length=" + message.request_params.length()
                    + "\nmessage.request_url.length=" + message.request_url.length()
                    + "\nmessage.request_user_agent.length=" + message.request_user_agent.length()
                    + "\nmessage.response_body.length=" + message.response_body.length()
                    + "\nmessage.response_code.length=" + message.response_code.length()
                    + "\nmessage.response_content_type.length=" + message.response_content_type.length()
                    + "\nmessage.response_headers.length=" + message.response_headers.length()
                    + "\nmessage.response_json_type.length=" + message.response_json_type.length()
                    + "\nmessage.custom_fields.length=" + message.custom_fields.length()
                    + "\nmessage.request_address.length=" + message.request_address.length()
                    + "\nmessage.session_fields.length=" + message.session_fields.length()
                    + "\nmessage.cookies.length=" + message.cookies.length()
                    + "\nmessage.id.offset=" + message.id.offset()
                    + "\nmessage.agent_category.offset=" + message.agent_category.offset()
                    + "\nmessage.agent_device.offset=" + message.agent_device.offset()
                    + "\nmessage.agent_name.offset=" + message.agent_name.offset()
                    + "\nmessage.graphql_operations.offset=" + message.graphql_operations.offset()
                    + "\nmessage.host.offset=" + message.host.offset()
                    + "\nmessage.request_body.offset=" + message.request_body.offset()
                    + "\nmessage.request_content_type.offset=" + message.request_content_type.offset()
                    + "\nmessage.request_headers.offset=" + message.request_headers.offset()
                    + "\nmessage.request_json_type.offset=" + message.request_json_type.offset()
                    + "\nmessage.request_method.offset=" + message.request_method.offset()
                    + "\nmessage.request_params.offset=" + message.request_params.offset()
                    + "\nmessage.request_url.offset=" + message.request_url.offset()
                    + "\nmessage.request_user_agent.offset=" + message.request_user_agent.offset()
                    + "\nmessage.response_body.offset=" + message.response_body.offset()
                    + "\nmessage.response_code.offset=" + message.response_code.offset()
                    + "\nmessage.response_content_type.offset=" + message.response_content_type.offset()
                    + "\nmessage.response_headers.offset=" + message.response_headers.offset()
                    + "\nmessage.response_json_type.offset=" + message.response_json_type.offset()
                    + "\nmessage.custom_fields.offset=" + message.custom_fields.offset()
                    + "\nmessage.request_address.offset=" + message.request_address.offset()
                    + "\nmessage.session_fields.offset=" + message.session_fields.offset()
                    + "\nmessage.cookies.offset=" + message.cookies.offset());
            return Slices.EMPTY_SLICE;
        }
    }

    @Override
    public Type getType(int field) {
        return column_types[field];
    }

    @Override
    public boolean isNull(int field) {
        switch (column_ordinal_positions[field]) {
            case 0:
                return message.id.isNull();
            case 1:
                return message.agent_category.isNull();
            case 2:
                return message.agent_device.isNull();
            case 3:
                return message.agent_name.isNull();
            case 4: // v3
                return message.graphql_operations.isNull();
            case 5: // v3
                return false;  // graphql_operations_count
            case 6:
                return message.host.isNull();
            case 7:
                return message.interval_millis.value() == 0;
            case 8:
                return message.request_body.isNull();
            case 9:
                return message.request_content_type.isNull();
            case 10:
                return message.request_headers.isNull();
            case 11:
                return message.request_json_type.isNull();
            case 12:
                return message.request_method.isNull();
            case 13:
                return message.request_params.isNull();
            case 14:
                return message.request_url.isNull();
            case 15:
                return message.request_user_agent.isNull();
            case 16:
                return message.response_body.isNull();
            case 17:
                return message.response_code.isNull();
            case 18:
                return message.response_content_type.isNull();
            case 19:
                return message.response_headers.isNull();
            case 20:
                return message.response_json_type.isNull();
            case 21:
                return message.response_time_millis.value() == 0;
            case 22:
                return message.size_request_bytes.value() == 0;
            case 23:
                return message.size_response_bytes.value() == 0;
            case 24: // v3
                return message.custom_fields.isNull();
            case 25: // v3
                return message.request_address.isNull();
            case 26: // v3
                return message.session_fields.isNull();
            case 27: // v3
                return message.cookies.isNull();
            case 28: // v3
                return false;  // cookies_count
            case 29: // v3.1
                return true;   // response_status
            case 30: // v3.1
                return false;  // size_total_bytes
            case 31: // v3.1
                return false;  // bitmap_versioning
            case 32: // v3.1
                return false;  // bitmap_request_info
            case 33: // v3.1
                return false;  // bitmap_request_json
            case 34: // v3.1
                return false;  // bitmap_request_graphql
            case 35: // v3.1
                return false;  // bitmap_request_pii
            case 36: // v3.1
                return false;  // bitmap_request_threat
            case 37: // v3.1
                return false;  // bitmap_response_info
            case 38: // v3.1
                return false;  // bitmap_response_json
            case 39: // v3.1
                return false;  // bitmap_response_pii
            case 40: // v3.1
                return false;  // bitmap_response_threat
            case 41: // v3.1
                return false;  // bitmap_attack_request
            case 42: // v3.1
                return false;  // bitmap_attack_application
            case 43: // v3.1
                return false;  // bitmap_attack_injection
            case 44: // v3.1
                return false;  // bitmap_response_leak
            case 45: // v3.1
                return false;  // bitmap_unused2
            case 46: // v3.1
                return false;  // bitmap_unused3
            case 47: // v3.1
                return false;  // bitmap_unused4
            case 48: // v3.1
                return false;  // bitmap_unused5
            case 49: // v3.5
                return false;  // shard_file
            case 50: // v3.6
                return false;  // size_host_bytes
            case 51: // v3.6
                return false;  // size_request_body_bytes
            case 52: // v3.6
                return false;  // size_request_headers_bytes
            case 53: // v3.6
                return false;  // size_request_params_bytes
            case 54: // v3.6
                return false;  // size_request_url_bytes
            case 55: // v3.6
                return false;  // size_response_body_bytes
            case 56: // v3.6
                return false;  // size_response_headers_bytes
            default:
                throw new IllegalArgumentException("Invalid field index: " + field);
        }
    }

}
