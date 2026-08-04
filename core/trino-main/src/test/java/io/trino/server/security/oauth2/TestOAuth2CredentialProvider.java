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
package io.trino.server.security.oauth2;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.InternalCommunicationEncryption;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Credential;
import io.trino.spi.security.OAuth2Credential;
import io.trino.spi.security.OAuth2CredentialRequest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.OK;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOAuth2CredentialProvider
{
    @Test
    public void testTokenExchange()
    {
        HttpClient httpClient = new TestingHttpClient(request -> {
            assertThat(request.getMethod()).isEqualTo("POST");
            assertThat(request.getUri().toASCIIString()).isEqualTo("https://server/realms/master/protocol/openid-connect/token");
            assertThat(request.getHeader(CONTENT_TYPE)).isEqualTo("application/x-www-form-urlencoded");

            String body = new String(((StaticBodyGenerator) request.getBodyGenerator()).getBody(), UTF_8);
            Map<String, String> formValues = new HashSet<>(Arrays.asList(body.split("&"))).stream()
                    .map(keyValue -> keyValue.split("="))
                    .collect(toMap(keyValue -> decode(keyValue[0], US_ASCII), keyValye -> decode(keyValye[1], US_ASCII)));

            assertThat(formValues).containsEntry("client_id", "trino-client");
            assertThat(formValues).containsEntry("client_secret", "trino-client-secret");
            assertThat(formValues).containsEntry("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
            assertThat(formValues).containsEntry("subject_token", "uvwxyz");
            assertThat(formValues).containsEntry("subject_token_type", "urn:ietf:params:oauth:token-type:access_token");
            assertThat(formValues).containsEntry("scope", "openid email");
            assertThat(formValues).containsEntry("audience", "external-client");

            return TestingResponse.mockResponse(OK, JSON_UTF_8,
                    """
                    {
                      "access_token": "abcd",
                      "expires_in": 60
                    }
                    """);
        });

        OAuth2Config config = new OAuth2Config()
                .setClientId("trino-client")
                .setClientSecret("trino-client-secret")
                .setTokenExchangeAllowed(true);

        OAuth2ServerConfigProvider.OAuth2ServerConfig serverConfig = new OAuth2ServerConfigProvider.OAuth2ServerConfig(
                Optional.empty(),
                URI.create("https://server/realms/master/protocol/openid-connect/auth"),
                URI.create("https://server/realms/master/protocol/openid-connect/token"),
                URI.create("https://server/realms/master/protocol/openid-connect/jwks.json"),
                Optional.empty(),
                Optional.empty());
        OAuth2ServerConfigProvider serverConfigProvider = () -> serverConfig;

        byte[] sharedSecret = new byte[512];
        new SecureRandom().nextBytes(sharedSecret);
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig()
                .setSharedSecret(Base64.getEncoder().encodeToString(sharedSecret));
        InternalCommunicationEncryption internalCommunicationEncryption = new InternalCommunicationEncryption(internalCommunicationConfig);

        OAuth2CredentialProvider provider = new OAuth2CredentialProvider(httpClient, config, serverConfigProvider, internalCommunicationEncryption);
        String audience = "external-client";
        String scope = "openid email";

        byte[] encryptedToken = internalCommunicationEncryption.encrypt("uvwxyz".getBytes(UTF_8));

        ConnectorIdentity connectorIdentity = ConnectorIdentity.forUser("user1").withExtraCredentials(Map.of("oauth_access_token", Base64.getEncoder().encodeToString(encryptedToken))).build();
        Credential credential = provider.getCredential(connectorIdentity, new OAuth2CredentialRequest(audience, scope)).get();
        assertThat(credential).isInstanceOf(OAuth2Credential.class);
        OAuth2Credential oauth2Credential = (OAuth2Credential) credential;
        assertThat(oauth2Credential.accessToken()).isEqualTo("abcd");
        assertThat(oauth2Credential.expiration()).isAfter(Instant.now());
        assertThat(oauth2Credential.expiration()).isBefore(Instant.now().plusSeconds(40));
    }
}
