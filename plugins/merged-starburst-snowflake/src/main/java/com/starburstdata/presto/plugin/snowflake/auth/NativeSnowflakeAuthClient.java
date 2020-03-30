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
package com.starburstdata.presto.plugin.snowflake.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.DocumentContext;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.AuthToLocal;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.spi.security.AccessDeniedException;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.executeWithRetries;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.jsonPath;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.parseJsonResponse;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NativeSnowflakeAuthClient
        implements SnowflakeAuthClient
{
    private static final Logger log = Logger.get(NativeSnowflakeAuthClient.class);

    private final SnowflakeOauthConfig config;
    private final Optional<AuthToLocal> authToLocal;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public NativeSnowflakeAuthClient(SnowflakeOauthConfig config)
    {
        this(config, Optional.empty());
    }

    public NativeSnowflakeAuthClient(SnowflakeOauthConfig config, AuthToLocal authToLocal)
    {
        this(config, Optional.of(authToLocal));
    }

    private NativeSnowflakeAuthClient(SnowflakeOauthConfig config, Optional<AuthToLocal> authToLocal)
    {
        this.config = requireNonNull(config, "config is null");
        this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getHttpConnectTimeout().roundTo(SECONDS), SECONDS)
                .readTimeout(config.getHttpReadTimeout().roundTo(SECONDS), SECONDS)
                .writeTimeout(config.getHttpWriteTimeout().roundTo(SECONDS), SECONDS)
                .build();
    }

    @Override
    public SamlRequest generateSamlRequest(JdbcIdentity identity)
    {
        try {
            log.debug("Generating Snowflake SAML request");
            // initiate SAML flow by asking SF to generate a SAML request
            String scope = authToLocal.map(authToLocal -> authToLocal.translate(identity))
                    .map("session:role:"::concat)
                    .orElse("");
            DocumentContext json = parseJsonResponse(
                    httpClient.newCall(
                            new Request.Builder()
                                    .url(format("%s/session/authenticate-request?__uiAppName=Login", config.getAccountUrl()))
                                    .post(RequestBody.create(
                                            MediaType.parse("application/json"),
                                            objectMapper.writeValueAsString(new SamlAuthenticateRequestPayload(ImmutableMap.<String, String>builder()
                                                    .put("ACCOUNT_NAME", config.getAccountName())
                                                    .put("clientId", config.getClientId())
                                                    .put("REAUTHENTICATION_TYPE", "FEDERATED")
                                                    .put("redirectUri", config.getRedirectUri())
                                                    .put("responseType", "code")
                                                    .put("scope", "refresh_token " + scope)
                                                    .build()))))
                                    .build()));
            // contains encoded SAML request
            String oktaRedirectUrl = jsonPath(json, "$.data.redirectUrl");
            String oAuthSessionStorageData = jsonPath(json, "$.data.oAuthSessionStorageData");
            return new SamlRequest(oktaRedirectUrl, oAuthSessionStorageData);
        }
        catch (IOException e) {
            log.error("Could not get SAML request from Snowflake", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public OauthCredential requestOauthToken(SamlResponse samlResponse)
    {
        try {
            log.debug("Using the SAML response to get a master token from Snowflake for user %s", samlResponse.getUser());
            DocumentContext sfAuthenticateResponseJson = parseJsonResponse(httpClient.newCall(
                    new Request.Builder()
                            .url(format("%s/session/authenticate-request?__uiAppName=Login", config.getAccountUrl()))
                            .post(RequestBody.create(
                                    MediaType.parse("application/json"),
                                    objectMapper.writeValueAsString(new SamlAuthenticateRequestPayload(ImmutableMap.<String, String>builder()
                                            .put("ACCOUNT_NAME", config.getClientId())
                                            .put("OAUTH_FEDERATED_CTX", samlResponse.getOauthSessionStorageData())
                                            .put("SAML_RESPONSE", samlResponse.getSamlAssertion())
                                            .build()))))
                            .build()));
            String masterToken = jsonPath(sfAuthenticateResponseJson, "$.data.masterToken");

            log.debug("Using the master token to get a code for OAuth on behalf of %s", samlResponse.getUser());
            DocumentContext sfAuthorizeJson = parseJsonResponse(
                    httpClient.newCall(
                            new Request.Builder()
                                    .url(format("%s/oauth/authorization-request", config.getAccountUrl()))
                                    .post(RequestBody.create(
                                            MediaType.parse("application/json"),
                                            objectMapper.writeValueAsString(new OauthAuthorizationRequestPayload(masterToken, config.getClientId()))))
                                    .build()));
            String redirectUrl = jsonPath(sfAuthorizeJson, "$.data.redirectUrl");
            // the redirect URL should look like http://localhost?code=XXX
            String code = new URIBuilder(redirectUrl)
                    .getQueryParams().stream()
                    .filter(param -> "code".equals(param.getName()))
                    .map(NameValuePair::getValue)
                    .findFirst()
                    .orElseThrow(() -> new AccessDeniedException(format(
                            "Code for obtaining OAuth token on behalf of %s not found in redirect URL %s",
                            samlResponse.getUser(),
                            redirectUrl)));

            // use the code to obtain the OAuth token
            log.debug("Using the code to get an OAuth token");
            long start = System.currentTimeMillis();
            Response tokenResponse = executeWithRetries(httpClient.newCall(
                    new Request.Builder()
                            .url(format("%s/oauth/token-request", config.getAccountUrl()))
                            .header("Authorization", Credentials.basic(config.getClientId(), config.getClientSecret()))
                            .post(new FormBody.Builder()
                                    .add("grant_type", "authorization_code")
                                    .add("code", code)
                                    .add("redirect_uri", config.getRedirectUri())
                                    .build())
                            .build()));
            // Snowflake responds with 400 for invalid client id/secret or bad authorization code
            if (tokenResponse.code() == 400) {
                log.error("OAuth token request call failed; response body was %s", tokenResponse.body().string());
                throw new AccessDeniedException("OAuth token request call failed");
            }
            DocumentContext tokenResponseJson = parseJsonResponse(tokenResponse);
            String accessToken = jsonPath(tokenResponseJson, "$.access_token");
            int accessTokenExpiresIn = jsonPath(tokenResponseJson, "$.expires_in");
            String refreshToken = jsonPath(tokenResponseJson, "$.refresh_token");
            int refreshTokenExpiresIn = jsonPath(tokenResponseJson, "$.refresh_token_expires_in");
            String snowflakeUsername = jsonPath(tokenResponseJson, "$.username");

            return new OauthCredential(
                    accessToken,
                    start + accessTokenExpiresIn * 1000,
                    refreshToken,
                    start + refreshTokenExpiresIn * 1000,
                    snowflakeUsername);
        }
        catch (AccessDeniedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain OAuth token for " + samlResponse.getUser(), e);
        }
    }

    @Override
    public OauthCredential refreshCredential(OauthCredential credential)
    {
        log.debug("Refreshing OAuth credential for %s", credential.getSnowflakeUsername());
        long start = System.currentTimeMillis();
        if (credential.getRefreshTokenExpirationTime() <= start) {
            log.error("Refresh token for %s is too old; it expired at: %s", credential.getSnowflakeUsername(), Instant.ofEpochMilli(credential.getRefreshTokenExpirationTime()));
            throw new AccessDeniedException("Refresh token too old");
        }
        try {
            DocumentContext refreshTokenResponseJson = parseJsonResponse(
                    httpClient.newCall(
                            new Request.Builder()
                                    .url(format("%s/oauth/token-request", config.getAccountUrl()))
                                    .header("Authorization", Credentials.basic(config.getClientId(), config.getClientSecret()))
                                    .post(new FormBody.Builder()
                                            .add("grant_type", "refresh_token")
                                            .add("refresh_token", credential.getRefreshToken())
                                            .build())
                                    .build()));
            String accessToken = jsonPath(refreshTokenResponseJson, "$.access_token");
            int accessTokenExpiresIn = jsonPath(refreshTokenResponseJson, "$.expires_in");

            return new OauthCredential(
                    accessToken,
                    start + accessTokenExpiresIn * 1000,
                    credential.getRefreshToken(),
                    credential.getRefreshTokenExpirationTime(),
                    credential.getSnowflakeUsername());
        }
        catch (AccessDeniedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to refresh OAuth token for " + credential.getSnowflakeUsername(), e);
        }
    }

    private static class SamlAuthenticateRequestPayload
    {
        private final Map<String, String> data;

        private SamlAuthenticateRequestPayload(Map<String, String> data)
        {
            this.data = data;
        }

        @JsonProperty("data")
        public Map<String, String> getData()
        {
            return data;
        }
    }

    private static class OauthAuthorizationRequestPayload
    {
        private String masterToken;
        private String clientId;

        public OauthAuthorizationRequestPayload(String masterToken, String clientId)
        {
            this.masterToken = masterToken;
            this.clientId = clientId;
        }

        @JsonProperty("masterToken")
        public String getMasterToken()
        {
            return masterToken;
        }

        @JsonProperty("clientId")
        public String getClientId()
        {
            return clientId;
        }

        @JsonProperty("stateToken")
        public String getRedirectUri()
        {
            return "http://localhost";
        }

        @JsonProperty("redirectUri")
        public String getResponseType()
        {
            return "code";
        }

        @JsonProperty("scope")
        public String getScope()
        {
            return "refresh_token";
        }
    }
}
