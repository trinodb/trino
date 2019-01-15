/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubole.presto.kinesis;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This factory class creates the KinesisConnector during server start and binds all the dependency
 * by calling create() method.
 */
public class KinesisConnectorFactory
        implements ConnectorFactory
{
    public static final String connectorName = "kinesis";
    private static final Logger log = Logger.get(KinesisConnectorFactory.class);

    private final ClassLoader classLoader;
    private TypeManager typeManager;
    private NodeManager nodeManager;
    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private Optional<Class<? extends KinesisClientProvider>> altProviderClass = Optional.empty();

    private KinesisHandleResolver handleResolver;

    private Injector injector;

    KinesisConnectorFactory(ClassLoader classLoader, Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier,
            Map<String, String> optionalConfig,
            Optional<Class<? extends KinesisClientProvider>> altProviderClass)
    {
        this.classLoader = classLoader;
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
        this.altProviderClass = requireNonNull(altProviderClass, "altProviderClass is null");

        this.handleResolver = new KinesisHandleResolver(connectorName);

        // Explanation: AWS uses a newer version of jackson (2.6.6) than airlift (2.4.4).  In order to upgrade
        // to the latest version of the AWS API, we need to turn this feature off.  This can be set
        // in jvm.properties but trying to make this more foolproof.
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");
    }

    @Override
    public String getName()
    {
        return connectorName;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KinesisHandleResolver(connectorName);
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        log.info("In connector factory create method.  Connector id: " + connectorId);
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KinesisConnectorModule(),
                    binder -> {
                        binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);
                        binder.bind(ConnectorId.class).toInstance(new ConnectorId(connectorId));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        // Note: moved creation from KinesisConnectorModule because connector manager accesses it earlier!
                        binder.bind(KinesisHandleResolver.class).toInstance(new KinesisHandleResolver(connectorName));

                        // Moved creation here from KinesisConnectorModule to make it easier to parameterize
                        if (altProviderClass.isPresent()) {
                            binder.bind(KinesisClientProvider.class).to(altProviderClass.get()).in(Scopes.SINGLETON);
                        }
                        else {
                            binder.bind(KinesisClientProvider.class).to(KinesisClientManager.class).in(Scopes.SINGLETON);
                        }

                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                        }
                        else {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).to(KinesisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    });

            this.injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            KinesisConnector connector = this.injector.getInstance(KinesisConnector.class);

            // Register objects for shutdown, at the moment only KinesisTableDescriptionSupplier
            if (!tableDescriptionSupplier.isPresent()) {
                // This will shutdown related dependent objects as well:
                KinesisTableDescriptionSupplier supp = getTableDescSupplier(this.injector);
                connector.registerShutdownObject(supp);
            }

            log.info("Done with injector.  Returning the connector itself.");
            return connector;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Convenience method to get the table description supplier.
     *
     * @param inj
     * @return
     */
    protected KinesisTableDescriptionSupplier getTableDescSupplier(Injector inj)
    {
        requireNonNull(inj, "Injector is missing in getTableDescSupplier");
        Supplier<Map<SchemaTableName, KinesisStreamDescription>> supplier =
                inj.getInstance(Key.get(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}));
        requireNonNull(inj, "Injector cannot find any table description supplier");
        return (KinesisTableDescriptionSupplier) supplier;
    }

    protected Injector getInjector()
    {
        return this.injector;
    }
}
