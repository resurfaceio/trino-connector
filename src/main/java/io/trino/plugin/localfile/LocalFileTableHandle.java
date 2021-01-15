// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LocalFileTableHandle implements ConnectorTableHandle {

    public LocalFileTableHandle(SchemaTableName schemaTableName, OptionalInt timestampColumn, OptionalInt serverAddressColumn) {
        this(schemaTableName, timestampColumn, serverAddressColumn, TupleDomain.all());
    }

    @JsonCreator
    public LocalFileTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("timestampColumn") OptionalInt timestampColumn,
            @JsonProperty("serverAddressColumn") OptionalInt serverAddressColumn,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.serverAddressColumn = requireNonNull(serverAddressColumn, "serverAddressColumn is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    private final TupleDomain<ColumnHandle> constraint;
    private final SchemaTableName schemaTableName;
    private final OptionalInt serverAddressColumn;
    private final OptionalInt timestampColumn;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalFileTableHandle that = (LocalFileTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(timestampColumn, that.timestampColumn) &&
                Objects.equals(serverAddressColumn, that.serverAddressColumn) &&
                Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, timestampColumn, serverAddressColumn, constraint);
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public OptionalInt getServerAddressColumn() {
        return serverAddressColumn;
    }

    @JsonProperty
    public OptionalInt getTimestampColumn() {
        return timestampColumn;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("schemaTableName", schemaTableName).toString();
    }

}
