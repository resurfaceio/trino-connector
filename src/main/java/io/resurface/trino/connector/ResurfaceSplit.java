// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ResurfaceSplit implements ConnectorSplit {

    @JsonCreator
    public ResurfaceSplit(@JsonProperty("address") HostAddress address, @JsonProperty("node_id") String node_id, @JsonProperty("slab") int slab) {
        this.address = requireNonNull(address, "address is null");
        this.node_id = requireNonNull(node_id, "node_id is null");
        this.slab = slab;
    }

    private final HostAddress address;
    private final String node_id;
    private final int slab;

    @JsonProperty
    public HostAddress getAddress() {
        return address;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @JsonProperty("node_id")
    public String getNodeId() {
        return node_id;
    }

    @JsonProperty
    public int getSlab() {
        return slab;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("address", address).add("node_id", node_id).toString();
    }

}
