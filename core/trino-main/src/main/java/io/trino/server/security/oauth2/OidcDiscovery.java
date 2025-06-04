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
import static io.trino.server.security.oauth2.StaticOAuth2ServerConfig.ACCESS_TOKEN_ISSUER;
import static io.trino.server.security.oauth2.StaticOAuth2ServerConfig.AUTH_URL;
import static io.trino.server.security.oauth2.StaticOAuth2ServerConfig.JWKS_URL;
import static io.trino.server.security.oauth2.StaticOAuth2ServerConfig.TOKEN_URL;
import static io.trino.server.security.oauth2.StaticOAuth2ServerConfig.USERINFO_URL;
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
    private final Optional<String> authUrl;
    private final Optional<String> tokenUrl;
    private final Optional<String> jwksUrl;
    private final Optional<String> userinfoUrl;
    private final NimbusHttpClient httpClient;

    @Inject
    public OidcDiscovery(OAuth2Config oauthConfig, OidcDiscoveryConfig oidcConfig, NimbusHttpClient httpClient)
    {
        issuer = new Issuer(requireNonNull(oauthConfig.getIssuer(), "issuer is null"));
        userinfoEndpointEnabled = oidcConfig.isUserinfoEndpointEnabled();
        discoveryTimeout = Duration.ofMillis(requireNonNull(oidcConfig.getDiscoveryTimeout(), "discoveryTimeout is null").toMillis());
        accessTokenIssuer = requireNonNull(oidcConfig.getAccessTokenIssuer(), "accessTokenIssuer is null");
        authUrl = requireNonNull(oidcConfig.getAuthUrl(), "authUrl is null");
        tokenUrl = requireNonNull(oidcConfig.getTokenUrl(), "tokenUrl is null");
        jwksUrl = requireNonNull(oidcConfig.getJwksUrl(), "jwksUrl is null");
        userinfoUrl = requireNonNull(oidcConfig.getUserinfoUrl(), "userinfoUrl is null");
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
        return readConfiguration(response.getBody());
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
                userinfoEndpoint = getOptionalField("userinfo_endpoint", Optional.ofNullable(metadata.getUserInfoEndpointURI()).map(URI::toString), USERINFO_URL, userinfoUrl);
            }
            else {
                userinfoEndpoint = Optional.empty();
            }
            Optional<URI> endSessionEndpoint = Optional.ofNullable(metadata.getEndSessionEndpointURI());
            return new OAuth2ServerConfig(
                    // AD FS server can include "access_token_issuer" field in OpenID Provider Metadata.
                    // It's not a part of the OIDC standard thus have to be handled separately.
                    // see: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-oidce/f629647a-4825-465b-80bb-32c7e9cec2c8
                    getOptionalField("access_token_issuer", Optional.ofNullable(metadataJson.get("access_token_issuer")).map(JsonNode::textValue), ACCESS_TOKEN_ISSUER, accessTokenIssuer),
                    getRequiredField("authorization_endpoint", metadata.getAuthorizationEndpointURI(), AUTH_URL, authUrl),
                    getRequiredField("token_endpoint", metadata.getTokenEndpointURI(), TOKEN_URL, tokenUrl),
                    getRequiredField("jwks_uri", metadata.getJWKSetURI(), JWKS_URL, jwksUrl),
                    userinfoEndpoint.map(URI::create),
                    endSessionEndpoint);
        }
        catch (JsonProcessingException e) {
            throw new ParseException("Invalid JSON value", e);
        }
    }

    private static URI getRequiredField(String metadataField, URI metadataValue, String configurationField, Optional<String> configurationValue)
    {
        Optional<String> uri = getOptionalField(metadataField, Optional.ofNullable(metadataValue).map(URI::toString), configurationField, configurationValue);
        checkMetadataState(uri.isPresent(), "Missing required \"%s\" property.", metadataField);
        return URI.create(uri.get());
    }

    private static Optional<String> getOptionalField(String metadataField, Optional<String> metadataValue, String configurationField, Optional<String> configurationValue)
    {
        if (configurationValue.isEmpty()) {
            return metadataValue;
        }

        if (metadataValue.isEmpty()) {
            return configurationValue;
        }

        if (!configurationValue.equals(metadataValue)) {
            LOG.warn("Overriding \"%s=%s\" from OpenID metadata document with value \"%s=%s\" defined in configuration",
                    metadataField, metadataValue.orElse(""), configurationField, configurationValue.orElse(""));
        }
        else {
            LOG.warn("Provided redundant configuration property \"%s\" with the same value as \"%s\" field in OpenID metadata document",
                    configurationField, metadataField);
        }
        return configurationValue;
    }

    private static void checkMetadataState(boolean expression, String additionalMessage, String... additionalMessageArgs)
    {
        checkState(expression, "Invalid response from OpenID Metadata endpoint. " + additionalMessage, (Object[]) additionalMessageArgs);
    }
}
