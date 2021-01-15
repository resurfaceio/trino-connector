// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.connector.*;

public class LocalFileHandleResolver implements ConnectorHandleResolver {

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return LocalFileColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return LocalFileSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return LocalFileTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return LocalFileTransactionHandle.class;
    }

}
