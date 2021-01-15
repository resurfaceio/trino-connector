// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.connector.*;

public class ResurfaceHandleResolver implements ConnectorHandleResolver {

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return ResurfaceColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return ResurfaceSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return ResurfaceTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return ResurfaceTransactionHandle.class;
    }

}
