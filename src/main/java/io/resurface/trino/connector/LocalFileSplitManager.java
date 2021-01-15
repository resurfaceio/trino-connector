// © 2016-2021 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.NodeManager;
import io.trino.spi.connector.*;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class LocalFileSplitManager implements ConnectorSplitManager {

    @Inject
    public LocalFileSplitManager(NodeManager nodeManager) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    private final NodeManager nodeManager;

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy,
                                          DynamicFilter dynamicFilter) {

        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .map(node -> new LocalFileSplit(node.getHostAndPort()))
                .collect(Collectors.toList());

        return new FixedSplitSource(splits);
    }

}
