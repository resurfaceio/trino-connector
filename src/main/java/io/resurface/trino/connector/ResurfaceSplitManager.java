// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.*;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResurfaceSplitManager implements ConnectorSplitManager {

    @Inject
    public ResurfaceSplitManager(ResurfaceConfig config, NodeManager nodeManager) {
        this.config = config;
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    private final ResurfaceConfig config;
    private final NodeManager nodeManager;

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
        List<ConnectorSplit> splits = new ArrayList<>();
        for (Node node : nodeManager.getAllNodes()) {
            for (int i = 1; i <= config.getMessagesSlabs(); i++) {
                splits.add(new ResurfaceSplit(node.getHostAndPort(), node.getNodeIdentifier(), i));
            }
        }
        return new FixedSplitSource(splits);
    }

}
