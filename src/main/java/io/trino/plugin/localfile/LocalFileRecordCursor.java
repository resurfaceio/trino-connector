// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.io.*;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.localfile.LocalFileColumnHandle.SERVER_ADDRESS_ORDINAL_POSITION;
import static io.trino.plugin.localfile.LocalFileErrorCode.LOCAL_FILE_READ_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static java.util.zip.GZIPInputStream.GZIP_MAGIC;

public class LocalFileRecordCursor implements RecordCursor {

    private static final Splitter LINE_SPLITTER = Splitter.on("\t").trimResults();

    // TODO This should be a config option as it may be different for different log files
    public static final DateTimeFormatter ISO_FORMATTER = ISO_OFFSET_DATE_TIME;

    public LocalFileRecordCursor(LocalFileTables localFileTables, List<LocalFileColumnHandle> columns,
                                 SchemaTableName tableName, HostAddress address,
                                 TupleDomain<LocalFileColumnHandle> predicate) {
        this.columns = requireNonNull(columns, "columns is null");
        this.address = requireNonNull(address, "address is null");

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            LocalFileColumnHandle columnHandle = columns.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        this.includeServer = isThisServerIncluded(address, predicate, localFileTables.getTable(tableName));
        this.reader = includeServer ? getFilesReader(localFileTables, predicate, tableName) : null;
    }

    private final HostAddress address;
    private final List<LocalFileColumnHandle> columns;
    private final int[] fieldToColumnIndex;
    private final boolean includeServer;
    private final FilesReader reader;
    private List<String> fields;

    private static FilesReader getFilesReader(LocalFileTables localFileTables, TupleDomain<LocalFileColumnHandle> predicate,
                                              SchemaTableName tableName) {
        LocalFileTableHandle table = localFileTables.getTable(tableName);
        List<File> fileNames = localFileTables.getFiles(tableName);
        try {
            return new FilesReader(table.getTimestampColumn(), fileNames.iterator(), predicate);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isThisServerIncluded(HostAddress address, TupleDomain<LocalFileColumnHandle> predicate,
                                                LocalFileTableHandle table) {
        if (table.getServerAddressColumn().isEmpty()) return true;
        Optional<Map<LocalFileColumnHandle, Domain>> domains = predicate.getDomains();
        if (domains.isEmpty()) return true;

        Set<Domain> serverAddressDomain = domains.get().entrySet().stream()
                .filter(entry -> entry.getKey().getOrdinalPosition() == table.getServerAddressColumn().getAsInt())
                .map(Map.Entry::getValue)
                .collect(toSet());

        if (serverAddressDomain.isEmpty()) return true;
        for (Domain domain : serverAddressDomain) {
            if (domain.includesNullableValue(Slices.utf8Slice(address.toString()))) return true;
        }
        return false;
    }

    @Override
    public boolean advanceNextPosition() {
        if (!includeServer) return false;
        try {
            fields = reader.readFields();
            return fields != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void checkFieldType(int field, Type... expected) {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) return;
        }
        String expectedTypes = Joiner.on(", ").join(expected);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", field, expectedTypes, actual));
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
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

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");
        int columnIndex = fieldToColumnIndex[field];
        if (columnIndex == SERVER_ADDRESS_ORDINAL_POSITION) return address.toString();
        if (columnIndex >= fields.size()) return null;
        return fields.get(columnIndex);
    }

    @Override
    public long getLong(int field) {
        if (getType(field).equals(createTimestampWithTimeZoneType(3))) {
            return parseTimestamp(getFieldValue(field));
        } else {
            checkFieldType(field, BIGINT, INTEGER);
            return Long.parseLong(getFieldValue(field));
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

    private static long parseTimestamp(String value) {
        OffsetDateTime time = OffsetDateTime.parse(value, ISO_FORMATTER)
                .plus(500, ChronoUnit.MICROS)
                .truncatedTo(ChronoUnit.MILLIS);
        long epochMillis = time.toInstant().toEpochMilli();
        int offsetMinutes = toIntExact(SECONDS.toMinutes(time.getOffset().getTotalSeconds()));
        return packDateTimeWithZone(epochMillis, offsetMinutes);
    }

    private static class FilesReader {

        public FilesReader(OptionalInt timestampOrdinalPosition, Iterator<File> files,
                           TupleDomain<LocalFileColumnHandle> predicate) throws IOException {
            requireNonNull(files, "files is null");
            this.files = files;
            requireNonNull(predicate, "predicate is null");
            this.domain = getDomain(timestampOrdinalPosition, predicate);
            this.timestampOrdinalPosition = timestampOrdinalPosition;
            reader = createNextReader();
        }

        private final Optional<Domain> domain;
        private final Iterator<File> files;
        private BufferedReader reader;
        private final OptionalInt timestampOrdinalPosition;

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
            InputStream in = isGZipped(file) ? new GZIPInputStream(fileInputStream) : fileInputStream;
            return new BufferedReader(new InputStreamReader(in));
        }

        private static Optional<Domain> getDomain(OptionalInt timestampOrdinalPosition,
                                                  TupleDomain<LocalFileColumnHandle> predicate) {
            Optional<Map<LocalFileColumnHandle, Domain>> domains = predicate.getDomains();
            Domain domain = null;
            if (domains.isPresent() && timestampOrdinalPosition.isPresent()) {
                Map<LocalFileColumnHandle, Domain> domainMap = domains.get();
                Set<Domain> timestampDomain = domainMap.entrySet().stream()
                        .filter(entry -> entry.getKey().getOrdinalPosition() == timestampOrdinalPosition.getAsInt())
                        .map(Map.Entry::getValue)
                        .collect(toSet());
                if (!timestampDomain.isEmpty()) domain = Iterables.getOnlyElement(timestampDomain);
            }
            return Optional.ofNullable(domain);
        }

        private static boolean isGZipped(File file) {
            try (RandomAccessFile inputFile = new RandomAccessFile(file, "r")) {
                int magic = inputFile.read() & 0xff | ((inputFile.read() << 8) & 0xff00);
                return magic == GZIP_MAGIC;
            } catch (IOException e) {
                throw new TrinoException(LOCAL_FILE_READ_ERROR, "Error reading file: " + file.getName(), e);
            }
        }

        private boolean meetsPredicate(List<String> fields) {
            if (timestampOrdinalPosition.isEmpty() || domain.isEmpty()) return true;
            long timestamp = parseTimestamp(fields.get(timestampOrdinalPosition.getAsInt()));
            return domain.get().includesNullableValue(timestamp);
        }

        public List<String> readFields() throws IOException {
            List<String> fields = null;
            boolean newReader = false;

            while (fields == null) {
                if (reader == null) return null;
                String line = reader.readLine();
                if (line != null) {
                    fields = LINE_SPLITTER.splitToList(line);
                    if (!newReader || meetsPredicate(fields)) return fields;
                }
                reader.close();
                reader = createNextReader();
                newReader = true;
            }

            return fields;
        }

    }

}
