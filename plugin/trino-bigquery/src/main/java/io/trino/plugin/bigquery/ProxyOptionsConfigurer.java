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

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Inject;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.grpc.netty.shaded.io.netty.handler.ssl.JdkSslContext;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkState;
import static io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.OPTIONAL;
import static java.util.Objects.requireNonNull;

public class ProxyOptionsConfigurer
        implements BigQueryGrpcOptionsConfigurer
{
    private final ProxyTransportFactory proxyTransportFactory;

    @Inject
    public ProxyOptionsConfigurer(ProxyTransportFactory proxyTransportFactory)
    {
        this.proxyTransportFactory = requireNonNull(proxyTransportFactory, "proxyTransportFactory is null");
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        return builder.setTransportOptions(proxyTransportFactory.getTransportOptions());
    }

    @Override
    public InstantiatingGrpcChannelProvider.Builder configure(InstantiatingGrpcChannelProvider.Builder channelBuilder, ConnectorSession session)
    {
        return channelBuilder.setChannelConfigurator(this::configureChannel);
    }

    private ManagedChannelBuilder configureChannel(ManagedChannelBuilder managedChannelBuilder)
    {
        checkState(managedChannelBuilder instanceof NettyChannelBuilder, "Expected ManagedChannelBuilder to be provider by Netty");
        NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) managedChannelBuilder;
        proxyTransportFactory.getSslContext().ifPresent(context -> {
            JdkSslContext jdkSslContext = new JdkSslContext(context, true, null, IdentityCipherSuiteFilter.INSTANCE, new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    "h2"), OPTIONAL);
            nettyChannelBuilder
                    .sslContext(jdkSslContext)
                    .useTransportSecurity();
        });

        return managedChannelBuilder.proxyDetector(proxyTransportFactory::createProxyDetector);
    }
}
