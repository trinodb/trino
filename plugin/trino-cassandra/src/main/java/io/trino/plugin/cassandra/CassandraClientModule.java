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
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonCodec;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.cassandra.v4_4.CassandraTelemetry;
import io.trino.plugin.cassandra.ptf.Query;
import io.trino.plugin.cassandra.tls.CassandraTlsModule;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static io.trino.plugin.cassandra.CassandraClientConfig.CassandraAuthenticationType.PASSWORD;
import static java.util.Objects.requireNonNull;

public class CassandraClientModule
        extends AbstractConfigurationAwareModule
{
    private final TypeManager typeManager;

    public CassandraClientModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void setup(Binder binder)
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
        newSetBinder(binder, Procedure.class).addBinding().toProvider(ExecuteProcedure.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTypeManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(CassandraClientConfig.class);

        install(conditionalModule(
                CassandraClientConfig.class,
                CassandraClientConfig::isTlsEnabled,
                new CassandraTlsModule()));

        install(conditionalModule(
                CassandraClientConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordAuthenticationModule()));

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, CassandraSessionConfigurator.class);

        closingBinder(binder).registerCloseable(CassandraSession.class);
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
    public static CassandraSession createCassandraSession(
            CassandraTypeManager cassandraTypeManager,
            CassandraClientConfig config,
            Set<CassandraSessionConfigurator> sessionConfigurators,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            OpenTelemetry openTelemetry)
    {
        requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder();

        List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
        checkArgument(!contactPoints.isEmpty(), "empty contactPoints");

        for (CassandraSessionConfigurator sessionConfigurator : sessionConfigurators) {
            sessionConfigurator.configure(cqlSessionBuilder);
        }

        return new CassandraSession(
                cassandraTypeManager,
                extraColumnMetadataCodec,
                () -> {
                    contactPoints.forEach(contactPoint -> cqlSessionBuilder.addContactPoint(
                            createInetSocketAddress(contactPoint, config.getNativeProtocolPort())));
                    CassandraTelemetry cassandraTelemetry = CassandraTelemetry.create(openTelemetry);
                    return cassandraTelemetry.wrap(cqlSessionBuilder.build());
                },
                config.getNoHostAvailableRetryTimeout());
    }

    @ProvidesIntoSet
    @Singleton
    public CassandraSessionConfigurator configurationLoaderConfigurator(CassandraClientConfig config)
    {
        return builder -> {
            ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
            // allow the retrieval of metadata for the system keyspaces
            driverConfigLoaderBuilder.withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, List.of());

            if (config.getProtocolVersion() != null) {
                driverConfigLoaderBuilder.withString(DefaultDriverOption.PROTOCOL_VERSION, config.getProtocolVersion().name());
            }

            driverConfigLoaderBuilder.withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy.class.getName());
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(500));
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(10));
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

            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, config.getClientReadTimeout().toJavaTime());
            driverConfigLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, config.getClientConnectTimeout().toJavaTime());
            if (config.getClientSoLinger() != null) {
                driverConfigLoaderBuilder.withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL, config.getClientSoLinger());
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

            builder.withConfigLoader(driverConfigLoaderBuilder.build());
        };
    }

    private static class PasswordAuthenticationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(CassandraPasswordConfig.class);
        }

        @ProvidesIntoSet
        @Singleton
        public CassandraSessionConfigurator passwordAuthenticationConfigurator(CassandraPasswordConfig config)
        {
            return builder -> builder.withAuthCredentials(config.getUsername(), config.getPassword());
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
