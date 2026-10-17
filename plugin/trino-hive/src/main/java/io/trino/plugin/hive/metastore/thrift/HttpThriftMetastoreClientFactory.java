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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.apachehttpclient.v5_2.ApacheHttpClientTelemetry;
import io.trino.spi.Node;
import jakarta.annotation.PreDestroy;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Creates metastore clients that send Thrift over HTTP or HTTPS, for {@code http://} and
 * {@code https://} metastore URIs. All clients share one pooled HTTP client.
 */
public class HttpThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private static final int MAX_CONNECTIONS = 100;

    private final String hostname;
    private final Optional<String> catalogName;
    private final CloseableHttpClient httpClient;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics = new MetastoreSupportsDateStatistics();
    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenTableParamAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterTransactionalTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterPartitionsAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenSetPartitionsColumnStatisticsAlternative = new AtomicInteger(Integer.MAX_VALUE);

    @Inject
    public HttpThriftMetastoreClientFactory(
            ThriftMetastoreConfig thriftConfig,
            ThriftHttpMetastoreConfig httpConfig,
            Node currentNode,
            OpenTelemetry openTelemetry)
    {
        this.hostname = requireNonNull(currentNode.getHost(), "hostname is null");
        this.catalogName = thriftConfig.getCatalogName();
        this.httpClient = createHttpClient(
                buildSslContext(thriftConfig),
                thriftConfig.getConnectTimeout(),
                thriftConfig.getReadTimeout(),
                headers(httpConfig),
                openTelemetry);
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        checkArgument(delegationToken.isEmpty(), "Delegation tokens are not supported for http(s) metastore URIs");
        return new ThriftHiveMetastoreClient(
                () -> new THttpClient(uri.toString(), httpClient),
                hostname,
                catalogName,
                metastoreSupportsDateStatistics,
                chosenGetTableAlternative,
                chosenTableParamAlternative,
                chosenAlterTransactionalTableAlternative,
                chosenAlterPartitionsAlternative,
                chosenSetPartitionsColumnStatisticsAlternative);
    }

    @PreDestroy
    public void shutdown()
    {
        httpClient.close(CloseMode.GRACEFUL);
    }

    private static Map<String, String> headers(ThriftHttpMetastoreConfig httpConfig)
    {
        // The bearer token is added last, so it replaces an Authorization entry in additional-headers
        Map<String, String> headers = new LinkedHashMap<>(httpConfig.getAdditionalHeaders());
        httpConfig.getBearerToken().ifPresent(token -> headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + token));
        return ImmutableMap.copyOf(headers);
    }

    private static CloseableHttpClient createHttpClient(
            SSLContext sslContext,
            Duration connectTimeout,
            Duration readTimeout,
            Map<String, String> headers,
            OpenTelemetry openTelemetry)
    {
        Timeout connectTimeoutMillis = Timeout.of(connectTimeout.toMillis(), MILLISECONDS);
        Timeout readTimeoutMillis = Timeout.of(readTimeout.toMillis(), MILLISECONDS);
        return ApacheHttpClientTelemetry.builder(openTelemetry).build().createHttpClientBuilder()
                .setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                        .setTlsSocketStrategy(new DefaultClientTlsStrategy(sslContext))
                        .setDefaultConnectionConfig(ConnectionConfig.custom()
                                .setConnectTimeout(connectTimeoutMillis)
                                .setSocketTimeout(readTimeoutMillis)
                                .build())
                        .setMaxConnPerRoute(MAX_CONNECTIONS)
                        .setMaxConnTotal(MAX_CONNECTIONS)
                        .build())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(connectTimeoutMillis)
                        .setResponseTimeout(readTimeoutMillis)
                        .build())
                // Retries are done by the Thrift metastore retry logic, and Thrift calls are not idempotent
                .disableAutomaticRetries()
                // Never send the bearer token or the additional headers to another location
                .disableRedirectHandling()
                .addRequestInterceptorFirst((request, _, _) -> headers.forEach(request::setHeader))
                .build();
    }

    private static SSLContext buildSslContext(ThriftMetastoreConfig config)
    {
        Optional<File> keystorePath = Optional.ofNullable(config.getKeystorePath());
        Optional<File> truststorePath = Optional.ofNullable(config.getTruststorePath());
        try {
            if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
                return SSLContext.getDefault();
            }
            return createSSLContext(
                    keystorePath,
                    Optional.ofNullable(config.getKeystorePassword()),
                    truststorePath,
                    Optional.ofNullable(config.getTruststorePassword()));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to create SSL context for the metastore HTTP client", e);
        }
    }
}
