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
package io.trino.plugin.elasticsearch.client;

import com.google.inject.Inject;
import io.airlift.stats.TimeStat;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import io.trino.spi.TrinoException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_SSL_INITIALIZATION_FAILURE;
import static java.lang.StrictMath.toIntExact;

public class ElasticsearchRestClientFactory
{
    private final Set<ElasticRestClientConfigurator> configurators;

    @Inject
    ElasticsearchRestClientFactory(Set<ElasticRestClientConfigurator> configurators)
    {
        this.configurators = configurators;
    }

    public BackpressureRestHighLevelClient createClient(ElasticsearchConfig config,
            TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                        config.getHosts().stream()
                                .map(httpHost -> new HttpHost(httpHost, config.getPort(), config.isTlsEnabled() ? "https" : "http"))
                                .toArray(HttpHost[]::new))
                .setMaxRetryTimeoutMillis(toIntExact(config.getMaxRetryTime().toMillis()));

        builder.setHttpClientConfigCallback(ignored -> {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(toIntExact(config.getConnectTimeout().toMillis()))
                    .setSocketTimeout(toIntExact(config.getRequestTimeout().toMillis()))
                    .build();

            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            // the client builder passed to the call-back is configured to use system properties, which makes it
            // impossible to configure concurrency settings, so we need to build a new one from scratch
            HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultIOReactorConfig(reactorConfig)
                    .setMaxConnPerRoute(config.getMaxHttpConnections())
                    .setMaxConnTotal(config.getMaxHttpConnections());

            if (config.isTlsEnabled()) {
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword())
                        .ifPresent(clientBuilder::setSSLContext);

                if (config.isVerifyHostnames()) {
                    clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
            }

            configurators.forEach(c -> c.configure(clientBuilder));

            return clientBuilder;
        });

        return new BackpressureRestHighLevelClient(new RestHighLevelClient(builder), config, backpressureStats);
    }

    private Optional<SSLContext> buildSslContext(
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(ELASTICSEARCH_SSL_INITIALIZATION_FAILURE, e);
        }
    }
}
