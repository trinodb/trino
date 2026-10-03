package tech.ydb.trino;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public record YdbConnectorFactory(String name, Supplier<Module> module) implements ConnectorFactory {

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
        checkStrictSpiVersionMatch(context, this);
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new ConnectorContextModule(catalogName, context),
                new JdbcModule(),
                module.get());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(YdbConnector.class);
    }
}
