/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import io.airlift.log.Logger;
import io.prestosql.spi.security.AccessDeniedException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.Call;
import okhttp3.JavaNetCookieJar;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.client.utils.URIBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.inject.Inject;

import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.executeWithRetries;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.jsonPath;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.parseJsonResponse;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.retryPolicy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NativeOktaAuthClient
        implements OktaAuthClient
{
    private static final Logger log = Logger.get(NativeOktaAuthClient.class);

    private final CookieManager cookieManager;
    private final OktaConfig oktaConfig;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public NativeOktaAuthClient(OktaConfig oktaConfig)
    {
        cookieManager = new CookieManager();
        this.oktaConfig = requireNonNull(oktaConfig, "oktaConfig is null");
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(oktaConfig.getHttpConnectTimeout().roundTo(SECONDS), SECONDS)
                .readTimeout(oktaConfig.getHttpReadTimeout().roundTo(SECONDS), SECONDS)
                .writeTimeout(oktaConfig.getHttpWriteTimeout().roundTo(SECONDS), SECONDS)
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();
    }

    @Override
    public OktaAuthenticationResult authenticate(String user, String password)
    {
        try {
            log.debug("Verifying credentials for user %s with Okta", user);
            Response response = executeWithRetries(httpClient.newCall(
                    new Request.Builder()
                            .url(format("%s/api/v1/authn", oktaConfig.getAccountUrl()))
                            .post(RequestBody.create(
                                    MediaType.parse("application/json"),
                                    objectMapper.writeValueAsString(new AuthenticationPayload(user, password))))
                            .build()));
            if (response.code() == 401) {
                log.error("Authentication failed for user %s; response body was %s", user, response.body().string());
                throw new AccessDeniedException("Invalid credentials for user " + user);
            }
            DocumentContext authenticationResult = parseJsonResponse(response);
            String status = jsonPath(authenticationResult, "$.status");
            if ("SUCCESS".equalsIgnoreCase(status)) {
                // MFA is not enabled
                log.debug("Successfully verified credentials for user %s with Okta (no MFA)", user);
                return new OktaAuthenticationResult(jsonPath(authenticationResult, "$.sessionToken"), user);
            }
            else if ("MFA_REQUIRED".equalsIgnoreCase(status)) {
                String stateToken = jsonPath(authenticationResult, "$.stateToken");
                // find the device where the MFA push notification should be sent
                List<String> factors = jsonPath(authenticationResult, "$._embedded.factors[?(@.factorType == \"push\")].id");

                log.debug("Sending push notification for user %s", user);
                Call pushMfa = httpClient.newCall(
                        new Request.Builder()
                                .url(format("%s/api/v1/authn/factors/%s/verify", oktaConfig.getAccountUrl(), factors.get(0)))
                                .post(RequestBody.create(
                                        MediaType.parse("application/json"),
                                        objectMapper.writeValueAsString(new VerifyPayload(stateToken))))
                                .build());
                // poll for successful MFA
                String sessionToken = Failsafe
                        .with(new RetryPolicy<>()
                                .withDelay(Duration.ofSeconds(2))
                                .withMaxDuration(Duration.ofMinutes(2))
                                .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                                .onRetry(event -> log.debug("Waiting for MFA authorization for user %s [%s]...", user, event.getLastFailure().getMessage())))
                        .get(() -> {
                            DocumentContext pushMfaResponseJson = parseJsonResponse(pushMfa.clone());
                            String mfaStatus = jsonPath(pushMfaResponseJson, "$.status");
                            if ("SUCCESS".equals(mfaStatus)) {
                                log.debug("User %s approved MFA push notification", user);
                                return jsonPath(pushMfaResponseJson, "$.sessionToken");
                            }
                            throw new AccessDeniedException(format("Failed to get MFA authorization for user %s [status = %s]", user, status));
                        });

                return new OktaAuthenticationResult(stateToken, sessionToken, user);
            }
            throw new AccessDeniedException(format("Invalid authentication status for user %s: %s", user, status));
        }
        catch (AccessDeniedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(format("Okta authentication failed for user %s", user), e);
        }
    }

    @Override
    public SamlResponse obtainSamlAssertion(SamlRequest samlRequest, OktaAuthenticationResult authenticationResult)
    {
        try {
            URL sessionCookieRedirectUri = new URIBuilder(oktaConfig.getAccountUrl())
                    .setPath("/login/sessionCookieRedirect")
                    .addParameter("token", authenticationResult.getSessionToken())
                    .addParameter("redirectUrl", samlRequest.getRedirectUrl())
                    .addParameter("checkAccountSetupComplete", "true")
                    .build()
                    .toURL();
            log.debug("Using state token to get a session id for user %s", authenticationResult.getUser());
            Request.Builder builder = new Request.Builder()
                    .url(sessionCookieRedirectUri)
                    .get();
            // state token is present only if MFA is enabled
            if (authenticationResult.getStateToken().isPresent()) {
                builder.addHeader("Cookie", format("oktaStateToken=%s", authenticationResult.getStateToken().get()));
            }
            // we're ignoring the response body because the information we need back from this call is returned as a cookie
            httpClient.newCall(builder.build()).execute().body().close();
            String sessionCookie = cookieManager.getCookieStore()
                    .getCookies()
                    .stream()
                    .filter(cookie -> "sid".equals(cookie.getName()) && !isNullOrEmpty(cookie.getValue()))
                    .findFirst().map(HttpCookie::getValue)
                    .orElseThrow(() -> new AccessDeniedException(format("HTTP response is missing session cookie for %s", authenticationResult.getUser())));

            // get the SAML response using the session id cookie to prove our identity
            Call saml = httpClient.newCall(
                    new Request.Builder()
                            .url(new URIBuilder(samlRequest.getRedirectUrl())
                                    .addParameter("fromLoginToken", authenticationResult.getSessionToken())
                                    .build()
                                    .toURL())
                            .header("Cookie", format("sid=%s", sessionCookie))
                            .get()
                            .build());
            String html = Failsafe.with(retryPolicy().abortOn(AccessDeniedException.class)).get(() -> {
                Response response = saml.execute();
                String responseBody = response.body().string();
                if (response.code() == 403) {
                    log.error("SAML Request denied for user %s; response body was %s", authenticationResult.getUser(), responseBody);
                    throw new AccessDeniedException(format("User %s is not assigned to Snowflake", authenticationResult.getUser()));
                }
                return responseBody;
            });
            // scrape the SAML response out of the HTML response
            Document doc = Jsoup.parse(html, StandardCharsets.UTF_8.name());
            String samAssertion = Optional.ofNullable(doc.selectFirst("form > input[name=SAMLResponse]"))
                    .map(element -> element.attr("value"))
                    // Element.attr() returns "" if the attribute is missing
                    .filter(attr -> !isNullOrEmpty(attr))
                    .orElseThrow(() -> {
                        log.error("Could not find SAMLResponse in page: %s", doc);
                        return new AccessDeniedException(format("SAMLResponse not present in page for user %s", authenticationResult.getUser()));
                    });
            return new SamlResponse(samAssertion, samlRequest.getOauthSessionStorageData(), authenticationResult.getUser());
        }
        catch (AccessDeniedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to get SAML assertion for user %s", authenticationResult.getUser()), e);
        }
    }

    private static class AuthenticationPayload
    {
        private final String username;
        private final String password;

        public AuthenticationPayload(String username, String password)
        {
            this.username = username;
            this.password = password;
        }

        @JsonProperty("username")
        public String getUsername()
        {
            return username;
        }

        @JsonProperty("password")
        public String getPassword()
        {
            return password;
        }
    }

    private static class VerifyPayload
    {
        private final String stateToken;

        public VerifyPayload(String stateToken)
        {
            this.stateToken = stateToken;
        }

        @JsonProperty("stateToken")
        public String getStateToken()
        {
            return stateToken;
        }
    }
}
