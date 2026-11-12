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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.testing.containers.FlociAzure;
import org.junit.jupiter.api.AutoClose;
import reactor.netty.resources.ConnectionProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;

abstract class AbstractTestAzureFileSystemFloci
        extends AbstractTestAzureFileSystem
{
    @AutoClose
    private final FlociAzureResource resource = new FlociAzureResource();

    @Override
    protected ClientSecretCredentialBuilder oauthCredentialBuilder()
    {
        return new ClientSecretCredentialBuilder()
                .authorityHost(resource.flociAzure().getEndpoint().toString())
                .httpClient(resource.httpClient())
                .disableInstanceDiscovery();
    }

    @Override
    protected AzureFileSystemFactory createFileSystemFactory(AzureAuth azureAuth, AzureFileSystemConfig config)
    {
        return new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                azureAuth,
                config.setServiceEndpoint(resource.flociAzure().getEndpoint()),
                Optional.of(resource.httpClient()));
    }

    @Override
    protected java.net.http.HttpClient createHttpClient()
    {
        return java.net.http.HttpClient.newBuilder()
                .sslContext(resource.sslContext())
                .build();
    }

    static final class FlociAzureResource
            implements AutoCloseable
    {
        private final FlociAzure flociAzure;
        private final ConnectionProvider connectionProvider = ConnectionProvider.create("floci-azure");
        private final HttpClient httpClient;
        private final SSLContext sslContext;

        public FlociAzureResource()
        {
            flociAzure = new FlociAzure();
            try {
                flociAzure.start();
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(null, null);
                trustStore.setCertificateEntry("floci-azure", flociAzure.getTlsCertificate());
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
                SslContext nettySslContext = SslContextBuilder.forClient()
                        .sslProvider(SslProvider.JDK)
                        .trustManager(trustManagerFactory)
                        .build();
                httpClient = new NettyAsyncHttpClientBuilder(reactor.netty.http.client.HttpClient.create(connectionProvider)
                        .secure(spec -> spec.sslContext(nettySslContext)))
                        .build();
            }
            catch (IOException | GeneralSecurityException | RuntimeException e) {
                connectionProvider.dispose();
                flociAzure.stop();
                throw new RuntimeException("Failed to trust Floci Azure TLS certificate", e);
            }
        }

        public FlociAzure flociAzure()
        {
            return flociAzure;
        }

        public HttpClient httpClient()
        {
            return httpClient;
        }

        public SSLContext sslContext()
        {
            return sslContext;
        }

        @Override
        public void close()
        {
            try {
                connectionProvider.disposeLater().block();
            }
            finally {
                flociAzure.stop();
            }
        }
    }
}
