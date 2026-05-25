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
package io.trino.plugin.iceberg.encryption;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.models.CreateRsaKeyOptions;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.OffsetDateTime;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;

@Testcontainers
final class TestIcebergAzureEncryptionFloci
        extends BaseIcebergEncryptionFlociTest
{
    @Container
    private static final AzureKeyVaultEmulator KEY_VAULT = new AzureKeyVaultEmulator();

    @Override
    protected String kmsKey()
    {
        String keyName = "test-key-" + randomNameSuffix();
        keyClient().createRsaKey(new CreateRsaKeyOptions(keyName).setKeySize(2048));
        return keyName;
    }

    @Override
    protected KeyManagementClient kmsClient()
    {
        return new EmulatorAzureKeyManagementClient(KEY_VAULT.endpoint());
    }

    private static KeyClient keyClient()
    {
        return new KeyClientBuilder()
                .vaultUrl(KEY_VAULT.endpoint().toString())
                .credential(new EmulatorTokenCredential(KEY_VAULT.endpoint()))
                .httpClient(insecureHttpClient())
                .disableChallengeResourceVerification()
                .buildClient();
    }

    private static com.azure.core.http.HttpClient insecureHttpClient()
    {
        reactor.netty.http.client.HttpClient nettyClient = reactor.netty.http.client.HttpClient.create()
                .secure(spec -> {
                    try {
                        spec.sslContext(SslContextBuilder.forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                .build());
                    }
                    catch (SSLException e) {
                        throw new RuntimeException(e);
                    }
                });
        return new NettyAsyncHttpClientBuilder(nettyClient).build();
    }

    private static final class EmulatorAzureKeyManagementClient
            implements KeyManagementClient
    {
        private final URI endpoint;

        private EmulatorAzureKeyManagementClient(URI endpoint)
        {
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
        }

        @Override
        public void initialize(Map<String, String> properties) {}

        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
        {
            byte[] wrapped = cryptographyClient(wrappingKeyId)
                    .wrapKey(KeyWrapAlgorithm.RSA_OAEP_256, toByteArray(key))
                    .getEncryptedKey();
            return ByteBuffer.wrap(wrapped);
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
        {
            byte[] unwrapped = cryptographyClient(wrappingKeyId)
                    .unwrapKey(KeyWrapAlgorithm.RSA_OAEP_256, toByteArray(wrappedKey))
                    .getKey();
            return ByteBuffer.wrap(unwrapped);
        }

        private CryptographyClient cryptographyClient(String keyName)
        {
            return new CryptographyClientBuilder()
                    .keyIdentifier(endpoint + "/keys/" + keyName)
                    .credential(new EmulatorTokenCredential(endpoint))
                    .httpClient(insecureHttpClient())
                    .disableChallengeResourceVerification()
                    .buildClient();
        }

        private static byte[] toByteArray(ByteBuffer buffer)
        {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

    private static final class EmulatorTokenCredential
            implements TokenCredential
    {
        private final URI endpoint;

        private EmulatorTokenCredential(URI endpoint)
        {
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
        }

        @Override
        public Mono<AccessToken> getToken(TokenRequestContext request)
        {
            try (HttpClient httpClient = insecureHttpClient()) {
                HttpResponse<String> response = httpClient.send(
                        HttpRequest.newBuilder(URI.create(endpoint + "/token")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                return Mono.just(new AccessToken(response.body().trim(), OffsetDateTime.now().plusMinutes(25)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private HttpClient insecureHttpClient()
        {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[] {
                        new X509TrustManager()
                        {
                            @Override
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

                            @Override
                            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

                            @Override
                            public X509Certificate[] getAcceptedIssuers()
                            {
                                return new X509Certificate[0];
                            }
                        },
                }, new SecureRandom());
                return HttpClient.newBuilder().sslContext(sslContext).build();
            }
            catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
