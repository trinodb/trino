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

import com.starburstdata.presto.okta.OktaAuthenticationResult;
import com.starburstdata.presto.okta.OktaClient;
import io.airlift.log.Logger;
import io.trino.spi.security.AccessDeniedException;
import net.jodah.failsafe.Failsafe;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.inject.Inject;

import java.io.IOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.starburstdata.presto.plugin.snowflake.auth.RestUtils.retryPolicy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NativeOktaAuthClient
        implements OktaAuthClient
{
    private static final Logger log = Logger.get(NativeOktaAuthClient.class);

    private final CookieManager cookieManager;
    private final HttpUrl accountUrl;
    private final OkHttpClient httpClient;
    private final OktaClient oktaClient;

    @Inject
    public NativeOktaAuthClient(OktaConfig oktaConfig)
    {
        cookieManager = new CookieManager();

        requireNonNull(oktaConfig, "oktaConfig is null");
        accountUrl = HttpUrl.parse(oktaConfig.getAccountUrl());
        checkArgument(accountUrl != null, "Invalid account URL: %s", oktaConfig.getAccountUrl());

        httpClient = new OkHttpClient.Builder()
                .connectTimeout(oktaConfig.getHttpConnectTimeout().roundTo(SECONDS), SECONDS)
                .readTimeout(oktaConfig.getHttpReadTimeout().roundTo(SECONDS), SECONDS)
                .writeTimeout(oktaConfig.getHttpWriteTimeout().roundTo(SECONDS), SECONDS)
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();
        oktaClient = new OktaClient(httpClient, accountUrl);
    }

    @Override
    public OktaAuthenticationResult authenticate(String user, String password)
    {
        return oktaClient.authenticate(user, password);
    }

    @Override
    public SamlResponse obtainSamlAssertion(SamlRequest samlRequest, OktaAuthenticationResult authenticationResult)
    {
        try {
            HttpUrl sessionCookieRedirectUri = accountUrl.newBuilder()
                    .encodedPath("/login/sessionCookieRedirect")
                    .addQueryParameter("token", authenticationResult.getSessionToken().getTokenValue())
                    .addQueryParameter("redirectUrl", samlRequest.getRedirectUrl())
                    .addQueryParameter("checkAccountSetupComplete", "true")
                    .build();
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
            HttpUrl redirectUrl = HttpUrl.parse(samlRequest.getRedirectUrl());
            checkArgument(redirectUrl != null, "Invalid redirect URL: %s", samlRequest.getRedirectUrl());
            Call saml = httpClient.newCall(
                    new Request.Builder()
                            .url(redirectUrl.newBuilder()
                                    .addQueryParameter("fromLoginToken", authenticationResult.getSessionToken().getTokenValue())
                                    .build())
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
        catch (IOException | RuntimeException e) {
            throw new RuntimeException(format("Failed to get SAML assertion for user %s", authenticationResult.getUser()), e);
        }
    }
}
