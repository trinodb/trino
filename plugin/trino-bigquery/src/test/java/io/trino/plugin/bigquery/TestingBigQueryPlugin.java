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
package io.trino.plugin.bigquery;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.grpc.ManagedChannelBuilder;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.guice.MBeanModule;

import java.net.URI;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class TestingBigQueryPlugin
        implements Plugin
{
    private final URI endpoint;

    public TestingBigQueryPlugin(URI endpoint)
    {
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "bigquery";
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                Injector injector = new Bootstrap(
                        "io.trino.bootstrap.catalog." + catalogName,
                        new JsonModule(),
                        new BigQueryConnectorModule(),
                        new MBeanServerModule(),
                        new MBeanModule(),
                        new ConnectorObjectNameGeneratorModule("io.trino.plugin.bigquery", "trino.plugin.bigquery"),
                        new ConnectorContextModule(catalogName, context),
                        binder -> newSetBinder(binder, BigQueryOptionsConfigurer.class).addBinding().toInstance(new FlociOptionsConfigurer(endpoint)))
                        .doNotInitializeLogging()
                        .disableSystemProperties()
                        .setRequiredConfigurationProperties(config)
                        .initialize();
                return injector.getInstance(BigQueryConnector.class);
            }
        });
    }

    public URI endpoint()
    {
        return endpoint;
    }

    private static final class FlociOptionsConfigurer
            implements BigQueryOptionsConfigurer
    {
        private final URI endpoint;

        private FlociOptionsConfigurer(URI endpoint)
        {
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
        }

        @Override
        public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
        {
            return builder
                    .setHost(endpoint.toString())
                    .setCredentials(NoCredentials.getInstance());
        }

        @Override
        public BigQueryReadSettings.Builder configure(BigQueryReadSettings.Builder builder, ConnectorSession session)
        {
            return builder
                    .setEndpoint(hostAndPort())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .setTransportChannelProvider(plaintext((InstantiatingGrpcChannelProvider) builder.getTransportChannelProvider()));
        }

        @Override
        public BigQueryWriteSettings.Builder configure(BigQueryWriteSettings.Builder builder, ConnectorSession session)
        {
            return builder
                    .setEndpoint(hostAndPort())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .setTransportChannelProvider(plaintext((InstantiatingGrpcChannelProvider) builder.getTransportChannelProvider()));
        }

        private InstantiatingGrpcChannelProvider plaintext(InstantiatingGrpcChannelProvider provider)
        {
            return provider.toBuilder()
                    .setEndpoint(hostAndPort())
                    .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                    .build();
        }

        private String hostAndPort()
        {
            return endpoint.getHost() + ":" + endpoint.getPort();
        }
    }
}
