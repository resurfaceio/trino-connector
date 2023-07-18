// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ResurfaceTableHandle implements ConnectorTableHandle {

    public ResurfaceTableHandle(SchemaTableName schemaTableName) {
        this(schemaTableName, TupleDomain.all());
    }

    @JsonCreator
    public ResurfaceTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    private final TupleDomain<ColumnHandle> constraint;
    private final SchemaTableName schemaTableName;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResurfaceTableHandle that = (ResurfaceTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) && Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, constraint);
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("schemaTableName", schemaTableName).toString();
    }

}
