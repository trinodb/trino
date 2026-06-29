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

import com.google.inject.Inject;
import io.airlift.http.client.FormDataBodyBuilder;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.security.credential.ForCredentialProvider;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Credential;
import io.trino.spi.security.CredentialProvider;
import io.trino.spi.security.CredentialRequest;
import io.trino.spi.security.OAuth2Credential;
import io.trino.spi.security.OAuth2CredentialRequest;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.Objects.requireNonNull;

public class OAuthCredentialProvider
        implements CredentialProvider
{
    private static final JsonCodec<Map<String, Object>> RESPONSE_CODEC = new JsonCodecFactory().mapJsonCodec(String.class, Object.class);
    private final Logger log = Logger.get(OAuthCredentialProvider.class);
    private final OAuth2Config config;
    private final OAuth2ServerConfigProvider.OAuth2ServerConfig serverConfig;
    private final HttpClient httpClient;

    @Inject
    public OAuthCredentialProvider(@ForCredentialProvider HttpClient httpClient, OAuth2Config config, OAuth2ServerConfigProvider serverConfigProvider)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.serverConfig = requireNonNull(serverConfigProvider.get(), "serverConfig is null");
    }

    @Override
    public Optional<Credential> getCredential(ConnectorIdentity identity, CredentialRequest credentialRequest)
    {
        // TODO add caching

        if (credentialRequest instanceof OAuth2CredentialRequest(String audience, String scope)) {
            String userToken = identity.getExtraCredentials().get("oauth_access_token");
            if (userToken == null) {
                return Optional.empty();
            }

            Request request = preparePost()
                    .setUri(serverConfig.tokenUrl())
                    .setMethod("POST")
                    .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .setBodyGenerator(new FormDataBodyBuilder()
                            .addField("client_id", config.getClientId())
                            .addField("client_secret", config.getClientSecret())
                            .addField("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
                            .addField("subject_token", userToken)
                            .addField("subject_token_type", "urn:ietf:params:oauth:token-type:access_token")
                            .addField("scope", scope)
                            .addField("audience", audience)
                            .build())
                    .build();

            StringResponseHandler.StringResponse response = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());

            if (response.getStatusCode() != 200) {
                throw new TrinoException(StandardErrorCode.CONFIGURATION_INVALID, serverConfig.tokenUrl() + " returned " + response.getStatusCode() + ": " + response.getBody());
            }

            Map<String, Object> json = RESPONSE_CODEC.fromJson(response.getBody());
                        String accessToken = (String) json.get("access_token");
                        int expiresIn = json.containsKey("expires_in") ? (int) json.get("expires_in") : 0;
                        Instant expiration = expiresIn > 0 ? Instant.now().plusSeconds(expiresIn) : Instant.now();
                        return Optional.of(new OAuth2Credential(accessToken, expiration));
//
//            log.info("Received response body " + response.getBody());
//
//            // TODO you can do better
//            String accessToken = response.getBody().substring(response.getBody().indexOf('"', response.getBody().indexOf("\"access_token\"") + "\"access_token\"".length()) + 1);
//            accessToken = accessToken.substring(0, accessToken.indexOf('"'));
//
//            log.info("This should be the access token: " + accessToken);
//            // TODO jsonCodec, parse expiration
//            return Optional.of(new OAuth2Credential(accessToken, Instant.now()));
        }
        return Optional.empty();
    }
}
