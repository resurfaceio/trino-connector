// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResurfaceRecordSetProvider implements ConnectorRecordSetProvider {

    @Inject
    public ResurfaceRecordSetProvider(ResurfaceTables tables) {
        this.tables = requireNonNull(tables, "tables is null");
    }

    private final ResurfaceTables tables;

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session,
                                  ConnectorSplit csplit, ConnectorTableHandle ctable,
                                  List<? extends ColumnHandle> columns) {
        ResurfaceSplit split = (ResurfaceSplit) csplit;
        ResurfaceTableHandle table = (ResurfaceTableHandle) ctable;
        ImmutableList.Builder<ResurfaceColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) handles.add((ResurfaceColumnHandle) handle);
        return new ResurfaceRecordSet(tables, split, table, handles.build());
    }

}
