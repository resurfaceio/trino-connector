// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalFileRecordSetProvider implements ConnectorRecordSetProvider {

    @Inject
    public LocalFileRecordSetProvider(LocalFileTables localFileTables) {
        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    private final LocalFileTables localFileTables;

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split,
                                  ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
        LocalFileSplit localFileSplit = (LocalFileSplit) split;
        LocalFileTableHandle localFileTable = (LocalFileTableHandle) table;
        ImmutableList.Builder<LocalFileColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) handles.add((LocalFileColumnHandle) handle);
        return new LocalFileRecordSet(localFileTables, localFileSplit, localFileTable, handles.build());
    }

}
