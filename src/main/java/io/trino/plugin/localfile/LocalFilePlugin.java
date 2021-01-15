// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class LocalFilePlugin implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new LocalFileConnectorFactory());
    }

}
