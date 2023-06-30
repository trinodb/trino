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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderConfigurationRequest;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpStatus.REQUEST_TIMEOUT;
import static io.airlift.http.client.HttpStatus.TOO_MANY_REQUESTS;
import static io.trino.server.security.oauth2.OAuth2Config.ACCESS_TOKEN_ISSUER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OidcDiscovery
        implements OAuth2ServerConfigProvider
{
    private static final Logger LOG = Logger.get(OidcDiscovery.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private final Issuer issuer;
    private final Duration discoveryTimeout;
    private final boolean userinfoEndpointEnabled;
    private final Optional<String> accessTokenIssuer;
    private final NimbusHttpClient httpClient;

    @Inject
    public OidcDiscovery(OAuth2Config oauthConfig, OidcDiscoveryConfig oidcConfig, NimbusHttpClient httpClient)
    {
        issuer = new Issuer(requireNonNull(oauthConfig.getIssuer(), "issuer is null"));
        userinfoEndpointEnabled = oidcConfig.isUserinfoEndpointEnabled();
        discoveryTimeout = Duration.ofMillis(requireNonNull(oidcConfig.getDiscoveryTimeout(), "discoveryTimeout is null").toMillis());
        accessTokenIssuer = requireNonNull(oauthConfig.getAccessTokenIssuer(), "accessTokenIssuer is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public OAuth2ServerConfig get()
    {
        return Failsafe.with(RetryPolicy.builder()
                        .withMaxAttempts(-1)
                        .withMaxDuration(discoveryTimeout)
                        .withDelay(Duration.ofSeconds(1))
                        .abortOn(IllegalStateException.class)
                        .onFailedAttempt(attempt -> LOG.debug("OpenID Connect Metadata read failed: %s", attempt.getLastException()))
                        .build())
                .get(() -> httpClient.execute(new OIDCProviderConfigurationRequest(issuer), this::parseConfigurationResponse));
    }

    private OAuth2ServerConfig parseConfigurationResponse(HTTPResponse response)
            throws ParseException
    {
        int statusCode = response.getStatusCode();
        if (statusCode != OK.code()) {
            // stop on any client errors other than REQUEST_TIMEOUT and TOO_MANY_REQUESTS
            if (statusCode < 400 || statusCode >= 500 || statusCode == REQUEST_TIMEOUT.code() || statusCode == TOO_MANY_REQUESTS.code()) {
                throw new RuntimeException("Invalid response from OpenID Metadata endpoint: " + statusCode);
            }
            throw new IllegalStateException(format("Invalid response from OpenID Metadata endpoint. Expected response code to be %s, but was %s", OK.code(), statusCode));
        }
        return readConfiguration(response.getContent());
    }

    private OAuth2ServerConfig readConfiguration(String body)
            throws ParseException
    {
        OIDCProviderMetadata metadata = OIDCProviderMetadata.parse(body);
        checkMetadataState(issuer.equals(metadata.getIssuer()), "The value of the \"issuer\" claim in Metadata document different than the Issuer URL used for the Configuration Request.");
        try {
            JsonNode metadataJson = OBJECT_MAPPER.readTree(body);
            Optional<String> userinfoEndpoint;
            if (userinfoEndpointEnabled) {
                userinfoEndpoint = Optional.ofNullable(metadata.getUserInfoEndpointURI()).map(URI::toString);
            }
            else {
                userinfoEndpoint = Optional.empty();
            }
            return new OAuth2ServerConfig(
                    // AD FS server can include "access_token_issuer" field in OpenID Provider Metadata.
                    // It's not a part of the OIDC standard thus have to be handled separately.
                    // see: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-oidce/f629647a-4825-465b-80bb-32c7e9cec2c8
                    getAccessTokenIssuer(metadataJson),
                    metadata.getAuthorizationEndpointURI(),
                    metadata.getTokenEndpointURI(),
                    metadata.getJWKSetURI(),
                    userinfoEndpoint.map(URI::create));
        }
        catch (JsonProcessingException e) {
            throw new ParseException("Invalid JSON value", e);
        }
    }

    private Optional<String> getAccessTokenIssuer(JsonNode metadata)
    {
        String metadataField = "access_token_issuer";
        Optional<String> metadataValue = Optional.ofNullable(metadata.get(metadataField)).map(JsonNode::textValue);
        if (accessTokenIssuer.isEmpty()) {
            return metadataValue;
        }

        if (metadataValue.isEmpty()) {
            return accessTokenIssuer;
        }

        if (!accessTokenIssuer.equals(metadataValue)) {
            LOG.warn("Overriding \"%s=%s\" from OpenID metadata document with value \"%s=%s\" defined in configuration",
                    metadataField, metadataValue.orElse(""), ACCESS_TOKEN_ISSUER, accessTokenIssuer.orElse(""));
        }
        else {
            LOG.warn("Provided redundant configuration property \"%s\" with the same value as \"%s\" field in OpenID metadata document",
                    accessTokenIssuer, metadataField);
        }
        return accessTokenIssuer;
    }

    private static void checkMetadataState(boolean expression, String additionalMessage, String... additionalMessageArgs)
    {
        checkState(expression, "Invalid response from OpenID Metadata endpoint. " + additionalMessage, (Object[]) additionalMessageArgs);
    }
}
