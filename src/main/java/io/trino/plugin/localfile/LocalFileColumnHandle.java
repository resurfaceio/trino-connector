// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LocalFileColumnHandle implements ColumnHandle {

    public static final int SERVER_ADDRESS_ORDINAL_POSITION = -1;
    public static final String SERVER_ADDRESS_COLUMN_NAME = "server_address";

    @JsonCreator
    public LocalFileColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition) {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
    }

    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalFileColumnHandle that = (LocalFileColumnHandle) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnType, that.columnType) &&
                Objects.equals(ordinalPosition, that.ordinalPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnType, ordinalPosition);
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public ColumnMetadata toColumnMetadata() {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }

}
