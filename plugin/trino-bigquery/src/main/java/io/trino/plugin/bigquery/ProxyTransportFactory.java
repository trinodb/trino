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

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.cloud.http.HttpTransportOptions;
import com.google.inject.Inject;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.trino.spi.TrinoException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_PROXY_SSL_INITIALIZATION_FAILED;
import static java.util.Objects.requireNonNull;

public interface ProxyTransportFactory
{
    HttpTransportOptions getTransportOptions();

    Optional<SSLContext> getSslContext();

    ProxiedSocketAddress createProxyDetector(SocketAddress socketAddress);

    class DefaultProxyTransportFactory
            implements ProxyTransportFactory
    {
        private final HttpTransportOptions transportOptions;
        private final Optional<SSLContext> sslContext;
        private final URI proxyUri;
        private final Optional<String> proxyUsername;
        private final Optional<String> proxyPassword;

        @Inject
        public DefaultProxyTransportFactory(BigQueryProxyConfig proxyConfig)
        {
            requireNonNull(proxyConfig, "proxyConfig is null");
            this.proxyUri = proxyConfig.getUri();
            this.proxyUsername = proxyConfig.getUsername();
            this.proxyPassword = proxyConfig.getPassword();

            this.sslContext = buildSslContext(proxyConfig.getKeystorePath(), proxyConfig.getKeystorePassword(), proxyConfig.getTruststorePath(), proxyConfig.getTruststorePassword());
            this.transportOptions = buildTransportOptions(sslContext, proxyUri, proxyUsername, proxyPassword);
        }

        @Override
        public HttpTransportOptions getTransportOptions()
        {
            return transportOptions;
        }

        @Override
        public Optional<SSLContext> getSslContext()
        {
            return sslContext;
        }

        @Override
        public ProxiedSocketAddress createProxyDetector(SocketAddress socketAddress)
        {
            HttpConnectProxiedSocketAddress.Builder builder = HttpConnectProxiedSocketAddress.newBuilder()
                    .setProxyAddress(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()))
                    .setTargetAddress((InetSocketAddress) socketAddress);

            proxyUsername.ifPresent(builder::setUsername);
            proxyPassword.ifPresent(builder::setPassword);

            return builder.build();
        }

        private static HttpTransportOptions buildTransportOptions(Optional<SSLContext> sslContext, URI proxyUri, Optional<String> proxyUser, Optional<String> proxyPassword)
        {
            HttpHost proxyHost = new HttpHost(proxyUri.getHost(), proxyUri.getPort());
            HttpRoutePlanner httpRoutePlanner = new DefaultProxyRoutePlanner(proxyHost);

            HttpClientBuilder httpClientBuilder = ApacheHttpTransport.newDefaultHttpClientBuilder()
                    .setRoutePlanner(httpRoutePlanner);

            if (sslContext.isPresent()) {
                SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext.get());
                httpClientBuilder.setSSLSocketFactory(sslSocketFactory);
            }

            if (proxyUser.isPresent()) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        new AuthScope(proxyHost.getHostName(), proxyHost.getPort()),
                        new UsernamePasswordCredentials(proxyUser.get(), proxyPassword.orElse("")));

                httpClientBuilder
                        .setProxyAuthenticationStrategy(ProxyAuthenticationStrategy.INSTANCE)
                        .setDefaultCredentialsProvider(credentialsProvider);
            }

            HttpClient client = httpClientBuilder.build(); // TODO: close http client on catalog deregistration
            return HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> new ApacheHttpTransport(client))
                    .build();
        }

        private static Optional<SSLContext> buildSslContext(
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
                throw new TrinoException(BIGQUERY_PROXY_SSL_INITIALIZATION_FAILED, e);
            }
        }
    }
}
