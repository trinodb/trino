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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.TransportSupplier;
import io.trino.spi.NodeManager;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DefaultThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String hostname;
    private final Optional<ThriftHttpContext> thriftHttpContext;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics = new MetastoreSupportsDateStatistics();
    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenTableParamAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsPerDatabaseAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterTransactionalTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterPartitionsAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllTablesAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsAlternative = new AtomicInteger(Integer.MAX_VALUE);

    public DefaultThriftMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration connectTimeout,
            Duration readTimeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname,
            Optional<ThriftHttpContext> thriftHttpContext)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.connectTimeoutMillis = toIntExact(connectTimeout.toMillis());
        this.readTimeoutMillis = toIntExact(readTimeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
        this.thriftHttpContext = requireNonNull(thriftHttpContext, "thriftHttpContext is null");
    }

    @Inject
    public DefaultThriftMetastoreClientFactory(
            ThriftMetastoreConfig config,
            Optional<ThriftHttpMetastoreConfig> httpMetastoreConfig,
            HiveMetastoreAuthentication metastoreAuthentication,
            NodeManager nodeManager)
    {
        this(
                buildSslContext(
                        config.isTlsEnabled(),
                        Optional.ofNullable(config.getKeystorePath()),
                        Optional.ofNullable(config.getKeystorePassword()),
                        Optional.ofNullable(config.getTruststorePath()),
                        Optional.ofNullable(config.getTruststorePassword())),
                Optional.ofNullable(config.getSocksProxy()),
                config.getConnectTimeout(),
                config.getReadTimeout(),
                metastoreAuthentication,
                nodeManager.getCurrentNode().getHost(),
                httpMetastoreConfig.map(DefaultThriftMetastoreClientFactory::buildThriftHttpContext));
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        return create(() -> getTransportSupplier(uri, delegationToken), hostname);
    }

    private TTransport getTransportSupplier(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        switch (uri.getScheme().toLowerCase(ENGLISH)) {
            case "thrift" -> {
                return createTransport(HostAndPort.fromParts(uri.getHost(), uri.getPort()), delegationToken);
            }
            case "http", "https" -> {
                return createHttpTransport(uri, thriftHttpContext.orElseThrow(() -> new IllegalArgumentException("Thrift http context is not set")));
            }
            default -> throw new IllegalArgumentException("Invalid metastore uri scheme " + uri.getScheme());
        }
    }

    private TTransport createHttpTransport(URI uri, ThriftHttpContext httpThriftContext)
            throws TTransportException
    {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        if (sslContext.isPresent()) {
            checkArgument(uri.getScheme().toLowerCase(ENGLISH).equals("https"), "URI must be https when setting SSLContext");
            checkArgument(httpThriftContext.token.isPresent(), "'hive.metastore.http.client.bearer-token' must be set when using https URI");
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext.get(), new DefaultHostnameVerifier(null));
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", socketFactory)
                    .build();
            httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
            httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> {
                httpRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + httpThriftContext.token.get());
            });
        }

        httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> {
            httpThriftContext.additionalHeaders().forEach(httpRequest::addHeader);
        });
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setResponseTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).build());
        return new THttpClient(uri.toString(), httpClientBuilder.build());
    }

    protected ThriftMetastoreClient create(TransportSupplier transportSupplier, String hostname)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(
                transportSupplier,
                hostname,
                metastoreSupportsDateStatistics,
                chosenGetTableAlternative,
                chosenTableParamAlternative,
                chosenGetAllTablesAlternative,
                chosenGetAllViewsPerDatabaseAlternative,
                chosenGetAllViewsAlternative,
                chosenAlterTransactionalTableAlternative,
                chosenAlterPartitionsAlternative);
    }

    private TTransport createTransport(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        return Transport.create(address, sslContext, socksProxy, connectTimeoutMillis, readTimeoutMillis, metastoreAuthentication, delegationToken);
    }

    private static Optional<SSLContext> buildSslContext(
            boolean tlsEnabled,
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private record ThriftHttpContext(Optional<String> token, Map<String, String> additionalHeaders)
    {
        private ThriftHttpContext
        {
            requireNonNull(additionalHeaders, "additionalHeaders is null");
            additionalHeaders = ImmutableMap.copyOf(additionalHeaders);
        }
    }

    @VisibleForTesting
    public static ThriftHttpContext buildThriftHttpContext(ThriftHttpMetastoreConfig config)
    {
        return new ThriftHttpContext(config.getHttpBearerToken(), config.getAdditionalHeaders());
    }
}
