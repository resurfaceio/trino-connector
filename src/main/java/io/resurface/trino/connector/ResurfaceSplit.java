// © 2016-2021 Resurface Labs Inc.

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
    public ResurfaceSplit(@JsonProperty("address") HostAddress address) {
        this.address = requireNonNull(address, "address is null");
    }

    private final HostAddress address;

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

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("address", address).toString();
    }

}