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

public class LocalFileTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final OptionalInt timestampColumn;
    private final OptionalInt serverAddressColumn;
    private final TupleDomain<ColumnHandle> constraint;

    public LocalFileTableHandle(SchemaTableName schemaTableName, OptionalInt timestampColumn, OptionalInt serverAddressColumn)
    {
        this(schemaTableName, timestampColumn, serverAddressColumn, TupleDomain.all());
    }

    @JsonCreator
    public LocalFileTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("timestampColumn") OptionalInt timestampColumn,
            @JsonProperty("serverAddressColumn") OptionalInt serverAddressColumn,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.serverAddressColumn = requireNonNull(serverAddressColumn, "serverAddressColumn is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public OptionalInt getTimestampColumn()
    {
        return timestampColumn;
    }

    @JsonProperty
    public OptionalInt getServerAddressColumn()
    {
        return serverAddressColumn;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalFileTableHandle that = (LocalFileTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(timestampColumn, that.timestampColumn) &&
                Objects.equals(serverAddressColumn, that.serverAddressColumn) &&
                Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, timestampColumn, serverAddressColumn, constraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .toString();
    }
}
