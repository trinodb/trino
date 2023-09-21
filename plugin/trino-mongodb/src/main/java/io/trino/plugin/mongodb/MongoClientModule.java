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
package io.trino.plugin.mongodb;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.mongo.v3_1.MongoTelemetry;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.mongodb.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MongoClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(MongoSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
        newSetBinder(binder, MongoClientSettingConfigurator.class);

        install(conditionalModule(
                MongoClientConfig.class,
                MongoClientConfig::getTlsEnabled,
                new MongoSslModule()));

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(TypeManager typeManager, MongoClientConfig config, Set<MongoClientSettingConfigurator> configurators, OpenTelemetry openTelemetry)
    {
        MongoClientSettings.Builder options = MongoClientSettings.builder();
        configurators.forEach(configurator -> configurator.configure(options));
        options.addCommandListener(MongoTelemetry.builder(openTelemetry).build().newCommandListener());
        MongoClient client = MongoClients.create(options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }

    @ProvidesIntoSet
    @Singleton
    public MongoClientSettingConfigurator defaultConfigurator(MongoClientConfig config)
    {
        return options -> {
            options.writeConcern(config.getWriteConcern().getWriteConcern())
                    .readPreference(config.getReadPreference().getReadPreference())
                    .applyToConnectionPoolSettings(builder -> builder
                            .maxConnectionIdleTime(config.getMaxConnectionIdleTime(), MILLISECONDS)
                            .maxWaitTime(config.getMaxWaitTime(), MILLISECONDS)
                            .minSize(config.getMinConnectionsPerHost())
                            .maxSize(config.getConnectionsPerHost()))
                    .applyToSocketSettings(builder -> builder
                            .connectTimeout(config.getConnectionTimeout(), MILLISECONDS)
                            .readTimeout(config.getSocketTimeout(), MILLISECONDS));

            if (config.getRequiredReplicaSetName() != null) {
                options.applyToClusterSettings(builder -> builder.requiredReplicaSetName(config.getRequiredReplicaSetName()));
            }
            options.applyConnectionString(new ConnectionString(config.getConnectionUrl()));
        };
    }
}
