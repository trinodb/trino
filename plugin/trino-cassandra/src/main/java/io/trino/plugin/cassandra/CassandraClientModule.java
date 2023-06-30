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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.json.JsonCodec;
import io.trino.plugin.cassandra.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_SSL_INITIALIZATION_FAILURE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CassandraClientModule
        implements Module
{
    private final TypeManager typeManager;

    public CassandraClientModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(CassandraConnector.class).in(Scopes.SINGLETON);
        binder.bind(CassandraMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTokenSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSessionProperties.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTypeManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(CassandraClientConfig.class);

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(TypeId.of(value));
        }
    }

    @Singleton
    @Provides
    public static CassandraSession createCassandraSession(CassandraTypeManager cassandraTypeManager, CassandraClientConfig config, JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec)
    {
        requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder();
        ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        // allow the retrieval of metadata for the system keyspaces
        driverConfigLoaderBuilder.withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, List.of());

        if (config.getProtocolVersion() != null) {
            driverConfigLoaderBuilder.withString(DefaultDriverOption.PROTOCOL_VERSION, config.getProtocolVersion().name());
        }

        List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
        checkArgument(!contactPoints.isEmpty(), "empty contactPoints");

        driverConfigLoaderBuilder.withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy.class.getName());
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(500));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofMillis(10_000));
        driverConfigLoaderBuilder.withString(DefaultDriverOption.RETRY_POLICY_CLASS, config.getRetryPolicy().getPolicyClass().getName());

        driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DefaultLoadBalancingPolicy.class.getName());
        if (config.isUseDCAware()) {
            requireNonNull(config.getDcAwareLocalDC(), "DCAwarePolicy localDC is null");
            driverConfigLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, config.getDcAwareLocalDC());

            if (config.getDcAwareUsedHostsPerRemoteDc() > 0) {
                driverConfigLoaderBuilder.withInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC, config.getDcAwareUsedHostsPerRemoteDc());
                if (config.isDcAwareAllowRemoteDCsForLocal()) {
                    driverConfigLoaderBuilder.withBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS, true);
                }
            }
        }

        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(toIntExact(config.getClientReadTimeout().toMillis())));
        driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(toIntExact(config.getClientConnectTimeout().toMillis())));
        if (config.getClientSoLinger() != null) {
            driverConfigLoaderBuilder.withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL, config.getClientSoLinger());
        }
        if (config.isTlsEnabled()) {
            buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                    .ifPresent(cqlSessionBuilder::withSslContext);
        }

        if (config.getUsername() != null && config.getPassword() != null) {
            cqlSessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
        }

        driverConfigLoaderBuilder.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, config.getFetchSize());
        driverConfigLoaderBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, config.getConsistencyLevel().name());

        if (config.getSpeculativeExecutionLimit().isPresent()) {
            driverConfigLoaderBuilder.withString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS, com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy.class.getName());
            // maximum number of executions
            driverConfigLoaderBuilder.withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, config.getSpeculativeExecutionLimit().get());
            // delay before a new execution is launched
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, Duration.ofMillis(config.getSpeculativeExecutionDelay().toMillis()));
        }

        cqlSessionBuilder.withConfigLoader(driverConfigLoaderBuilder.build());

        return new CassandraSession(
                cassandraTypeManager,
                extraColumnMetadataCodec,
                () -> {
                    contactPoints.forEach(contactPoint -> cqlSessionBuilder.addContactPoint(
                            createInetSocketAddress(contactPoint, config.getNativeProtocolPort())));
                    return cqlSessionBuilder.build();
                },
                config.getNoHostAvailableRetryTimeout());
    }

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
            throw new TrinoException(CASSANDRA_SSL_INITIALIZATION_FAILURE, e);
        }
    }

    private static InetSocketAddress createInetSocketAddress(String contactPoint, int port)
    {
        try {
            return new InetSocketAddress(InetAddress.getByName(contactPoint), port);
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException("Failed to add contact point: " + contactPoint, e);
        }
    }
}
