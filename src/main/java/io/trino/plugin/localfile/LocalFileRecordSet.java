// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalFileRecordSet
        implements RecordSet
{
    private final List<LocalFileColumnHandle> columns;
    private final List<Type> columnTypes;
    private final HostAddress address;
    private final TupleDomain<LocalFileColumnHandle> effectivePredicate;
    private final SchemaTableName tableName;
    private final LocalFileTables localFileTables;

    public LocalFileRecordSet(LocalFileTables localFileTables, LocalFileSplit split, LocalFileTableHandle table, List<LocalFileColumnHandle> columns)
    {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LocalFileColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.address = Iterables.getOnlyElement(split.getAddresses());
        this.effectivePredicate = table.getConstraint()
                .transform(LocalFileColumnHandle.class::cast);
        this.tableName = table.getSchemaTableName();

        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LocalFileRecordCursor(localFileTables, columns, tableName, address, effectivePredicate);
    }
}
