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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.apachehttpclient.v5_2.ApacheHttpClient5Telemetry;
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

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HttpThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final int readTimeoutMillis;
    private final String hostname;
    private final Optional<ThriftHttpMetastoreConfig.AuthenticationMode> authenticationMode;
    private final Optional<String> token;
    private final Map<String, String> additionalHeaders;
    private final OpenTelemetry openTelemetry;

    @Inject
    public HttpThriftMetastoreClientFactory(
            ThriftHttpMetastoreConfig httpMetastoreConfig,
            NodeManager nodeManager,
            OpenTelemetry openTelemetry)
    {
        this.readTimeoutMillis = toIntExact(httpMetastoreConfig.getReadTimeout().toMillis());
        this.hostname = requireNonNull(nodeManager.getCurrentNode().getHost(), "hostname is null");
        this.authenticationMode = httpMetastoreConfig.getAuthenticationMode();
        this.token = httpMetastoreConfig.getHttpBearerToken();
        this.additionalHeaders = ImmutableMap.copyOf(httpMetastoreConfig.getAdditionalHeaders());
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(
                () -> createHttpTransport(uri),
                hostname,
                new MetastoreSupportsDateStatistics(),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE),
                new AtomicInteger(Integer.MAX_VALUE));
    }

    private TTransport createHttpTransport(URI uri)
            throws TTransportException
    {
        HttpClientBuilder httpClientBuilder = ApacheHttpClient5Telemetry.builder(openTelemetry).build().newHttpClientBuilder();
        if ("https".equals(uri.getScheme().toLowerCase(ENGLISH))) {
            checkArgument(token.isPresent(), "'hive.metastore.http.client.bearer-token' must be set while using https metastore URIs in 'hive.metastore.uri'");
            checkArgument(authenticationMode.isPresent(), "'hive.metastore.http.client.authentication.type' must be set while using http/https metastore URIs in 'hive.metastore.uri'");
            SSLConnectionSocketFactory socketFactory;
            try {
                socketFactory = new SSLConnectionSocketFactory(SSLContext.getDefault(), new DefaultHostnameVerifier());
            }
            catch (NoSuchAlgorithmException e) {
                throw new TTransportException(e);
            }
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", socketFactory)
                    .build();
            httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
            httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> httpRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.get()));
        }
        else {
            checkArgument(token.isEmpty(), "'hive.metastore.http.client.bearer-token' must not be set while using http metastore URIs in 'hive.metastore.uri'");
        }
        httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> additionalHeaders.forEach(httpRequest::addHeader));
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setResponseTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).build());
        return new THttpClient(uri.toString(), httpClientBuilder.build());
    }
}
