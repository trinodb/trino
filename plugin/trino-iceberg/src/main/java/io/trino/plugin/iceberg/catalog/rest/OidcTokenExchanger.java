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
package io.trino.plugin.iceberg.catalog.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.cache.NonEvictableCache;
import io.trino.cache.SafeCaches;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;

public class OidcTokenExchanger
{
    private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";
    private static final long DEFAULT_EXPIRY_SECONDS = 3600;
    private static final long EXPIRY_BUFFER_SECONDS = 60;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    record CachedToken(String token, Instant expiresAt) {}

    private final URI endpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final Map<String, String> extraParams;
    private final HttpClient httpClient;
    private final NonEvictableCache<String, CachedToken> cache;

    @Inject
    public OidcTokenExchanger(
            IcebergRestCatalogTokenExchangeConfig config,
            @Named("tokenExchangeExtra") Map<String, String> extraParams)
    {
        this(config.getEndpoint().orElseThrow(),
                config.getClientId().orElseThrow(),
                config.getClientSecret().orElseThrow(),
                config.getScope().orElseThrow(),
                extraParams);
    }

    OidcTokenExchanger(URI endpoint, String clientId, String clientSecret, String scope)
    {
        this(endpoint, clientId, clientSecret, scope, Map.of());
    }

    OidcTokenExchanger(URI endpoint, String clientId, String clientSecret, String scope, Map<String, String> extraParams)
    {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.extraParams = Map.copyOf(extraParams);
        this.httpClient = HttpClient.newHttpClient();
        this.cache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    public String getToken(String oidcToken)
    {
        String sub = OidcStsCredentialExchanger.extractClaim(oidcToken, "sub");
        if (sub == null) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "OIDC token is missing required 'sub' claim");
        }
        CachedToken cached = cache.getIfPresent(sub);
        if (cached != null && cached.expiresAt().minusSeconds(EXPIRY_BUFFER_SECONDS).isAfter(Instant.now())) {
            return cached.token();
        }
        CachedToken fresh = exchange(oidcToken);
        cache.put(sub, fresh);
        return fresh.token();
    }

    CachedToken exchange(String oidcToken)
    {
        StringBuilder body = new StringBuilder()
                .append("grant_type=").append(encode(GRANT_TYPE))
                .append("&assertion=").append(encode(oidcToken))
                .append("&client_id=").append(encode(clientId))
                .append("&client_secret=").append(encode(clientSecret))
                .append("&scope=").append(encode(scope));

        extraParams.forEach((key, value) ->
                body.append("&").append(encode(key)).append("=").append(encode(value)));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Token exchange request interrupted", e);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Token exchange request failed", e);
        }

        if (response.statusCode() != 200) {
            throw new TrinoException(
                    ICEBERG_CATALOG_ERROR,
                    "Token exchange failed with HTTP " + response.statusCode() + ": " + response.body());
        }

        try {
            JsonNode json = MAPPER.readTree(response.body());
            String accessToken = json.path("access_token").asText(null);
            if (accessToken == null) {
                throw new TrinoException(ICEBERG_CATALOG_ERROR, "Token exchange response missing 'access_token'");
            }
            long expiresIn = json.path("expires_in").asLong(DEFAULT_EXPIRY_SECONDS);
            return new CachedToken(accessToken, Instant.now().plusSeconds(expiresIn));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to parse token exchange response", e);
        }
    }

    private static String encode(String value)
    {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
