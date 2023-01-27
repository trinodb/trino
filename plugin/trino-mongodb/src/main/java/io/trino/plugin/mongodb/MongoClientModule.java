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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.trino.plugin.mongodb.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;

import javax.inject.Singleton;
import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MongoClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(TypeManager typeManager, MongoClientConfig config)
    {
        MongoClientSettings.Builder options = MongoClientSettings.builder();
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
        if (config.getTlsEnabled()) {
            options.applyToSslSettings(builder -> {
                builder.enabled(true);
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                        .ifPresent(builder::context);
            });
        }

        options.applyConnectionString(new ConnectionString(config.getConnectionUrl()));

        MongoClient client = MongoClients.create(options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }

    // TODO https://github.com/trinodb/trino/issues/15247 Add test for x.509 certificates
    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
