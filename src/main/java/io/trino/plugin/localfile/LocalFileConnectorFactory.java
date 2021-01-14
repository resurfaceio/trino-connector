// © 2016-2021 Resurface Labs Inc.

package io.trino.plugin.localfile;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.resurface.binfiles.BinaryHttpMessage;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LocalFileConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        new BinaryHttpMessage(); // todo remove this, just testing classloader
        return "resurface";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LocalFileHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                new LocalFileModule());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(LocalFileConnector.class);
    }
}
