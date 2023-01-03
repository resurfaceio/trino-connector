// © 2016-2023 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class ResurfaceConnector implements Connector {

    @Inject
    public ResurfaceConnector(LifeCycleManager lifeCycleManager, ResurfaceMetadata metadata,
                              ResurfaceSplitManager splitManager, ResurfaceRecordSetProvider recordSetProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
    }

    private final LifeCycleManager lifeCycleManager;
    private final ResurfaceMetadata metadata;
    private final ResurfaceRecordSetProvider recordSetProvider;
    private final ResurfaceSplitManager splitManager;

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return ResurfaceTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }

}
