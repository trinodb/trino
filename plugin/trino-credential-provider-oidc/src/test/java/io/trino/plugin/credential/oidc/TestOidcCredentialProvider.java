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
package io.trino.plugin.credential.oidc;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BearerTokenCredential;
import io.trino.spi.security.credential.CredentialProvider;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.OK;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOidcCredentialProvider
{
    @Test
    public void testFactory()
    {
        OidcCredentialProviderFactory factory = new OidcCredentialProviderFactory();
        assertThat(factory.getFactoryName()).isEqualTo("oidc");

        CredentialProvider provider = factory.create("my-name", ImmutableMap.of(
                "client-id", "trino-client",
                "client-secret", "trino-client-secret",
                "scopes", "openid email",
                "audience", "external-client",
                "token-url", "https://server/realms/master/protocol/openid-connect/token"));
        assertThat(provider).isInstanceOf(OidcCredentialProvider.class);
    }

    @Test
    public void test()
    {
        HttpClient httpClient = new TestingHttpClient(request -> {
            assertThat(request.getMethod()).isEqualTo("POST");
            assertThat(request.getUri().toASCIIString()).isEqualTo("https://server/realms/master/protocol/openid-connect/token");
            assertThat(request.getHeader(CONTENT_TYPE)).isEqualTo("application/x-www-form-urlencoded");

            String body = new String(((StaticBodyGenerator) request.getBodyGenerator()).getBody(), UTF_8);
            Map<String, String> formValues = new HashSet<>(Arrays.asList(body.split("&"))).stream()
                    .map(keyValue -> keyValue.split("="))
                    .collect(toMap(keyValue -> decode(keyValue[0], US_ASCII), keyValue -> decode(keyValue[1], US_ASCII)));

            assertThat(formValues).containsEntry("client_id", "trino-client");
            assertThat(formValues).containsEntry("client_secret", "trino-client-secret");
            assertThat(formValues).containsEntry("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
            assertThat(formValues).containsEntry("subject_token", "the-base64-access-token");
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

        OidcCredentialProviderConfig config = new OidcCredentialProviderConfig()
                .setAudience("external-client")
                .setClientId("trino-client")
                .setClientSecret("trino-client-secret")
                .setScopes("openid email")
                .setTokenUrl(URI.create("https://server/realms/master/protocol/openid-connect/token"));

        OidcCredentialProvider provider = new OidcCredentialProvider(httpClient, config);
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice").withExtraCredentials(ImmutableMap.of("oauth2_access_token", "the-base64-access-token")).build();
        BearerTokenCredential credential = provider.getCredential(identity, BearerTokenCredential.class);

        assertThat(credential.getHeaders()).isEqualTo(ImmutableMap.of("Authorization", "Bearer abcd"));
        assertThat(credential.bearerToken()).isEqualTo("abcd");
        assertThat(credential.expiration()).isAfter(Instant.now());
        assertThat(credential.expiration()).isBefore(Instant.now().plusSeconds(40));
    }
}
