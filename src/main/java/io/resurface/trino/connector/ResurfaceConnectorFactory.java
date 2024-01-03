// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ResurfaceConnectorFactory implements ConnectorFactory {

    public static final String CONNECTOR_NAME = "resurface";

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                new ResurfaceModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ResurfaceConnector.class);
    }

    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }

}
