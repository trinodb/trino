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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SnowflakeOauthConfig
{
    private String accountUrl;
    private String accountName;
    private String clientId;
    private String clientSecret;
    private String redirectUri = "https://localhost";
    private int credentialCacheSize = 10000;
    private Duration httpConnectTimeout = new Duration(30, SECONDS);
    private Duration httpReadTimeout = new Duration(30, SECONDS);
    private Duration httpWriteTimeout = new Duration(30, SECONDS);
    private Duration credentialTtl;

    public int getCredentialCacheSize()
    {
        return credentialCacheSize;
    }

    @Config("snowflake.credential.cache-size")
    @ConfigDescription("Maximum number of OAuth tokens to cache")
    public SnowflakeOauthConfig setCredentialCacheSize(int credentialCacheSize)
    {
        this.credentialCacheSize = credentialCacheSize;
        return this;
    }

    public Duration getHttpConnectTimeout()
    {
        return httpConnectTimeout;
    }

    @Config("snowflake.credential.http-connect-timeout")
    @ConfigDescription("Connect timeout for HTTP calls")
    public SnowflakeOauthConfig setHttpConnectTimeout(Duration httpConnectTimeout)
    {
        this.httpConnectTimeout = httpConnectTimeout;
        return this;
    }

    public Duration getHttpReadTimeout()
    {
        return httpReadTimeout;
    }

    @Config("snowflake.credential.http-read-timeout")
    @ConfigDescription("Read timeout for HTTP calls")
    public SnowflakeOauthConfig setHttpReadTimeout(Duration httpReadTimeout)
    {
        this.httpReadTimeout = httpReadTimeout;
        return this;
    }

    public Duration getHttpWriteTimeout()
    {
        return httpWriteTimeout;
    }

    @Config("snowflake.credential.http-write-timeout")
    @ConfigDescription("Write timeout for HTTP calls")
    public SnowflakeOauthConfig setHttpWriteTimeout(Duration httpWriteTimeout)
    {
        this.httpWriteTimeout = httpWriteTimeout;
        return this;
    }

    @NotNull
    public Duration getCredentialTtl()
    {
        return credentialTtl;
    }

    @Config("snowflake.credential.cache-ttl")
    @ConfigDescription("Time after which the user should be prompted to reauthorize the connection (MFA push)")
    public SnowflakeOauthConfig setCredentialTtl(Duration credentialTtl)
    {
        this.credentialTtl = credentialTtl;
        return this;
    }

    @NotNull
    public String getAccountUrl()
    {
        return accountUrl;
    }

    @Config("snowflake.account-url")
    @ConfigDescription("Snowflake URL (https://your_sf_account_name.snowflakecomputing.com)")
    public SnowflakeOauthConfig setAccountUrl(String accountUrl)
    {
        this.accountUrl = accountUrl;
        return this;
    }

    @NotNull
    public String getAccountName()
    {
        return accountName;
    }

    @Config("snowflake.account-name")
    @ConfigDescription("Snowflake account name")
    public SnowflakeOauthConfig setAccountName(String accountName)
    {
        this.accountName = accountName;
        return this;
    }

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("snowflake.client-id")
    @ConfigDescription("Security integration client id")
    public SnowflakeOauthConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("snowflake.client-secret")
    @ConfigDescription("Security integration client secret")
    @ConfigSecuritySensitive
    public SnowflakeOauthConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public String getRedirectUri()
    {
        return redirectUri;
    }

    /**
     * When requesting an OAuth token for a security integration, the client needs to provide
     * the redirect URI.  This is important in a browser based flow, but we don't really use it.
     * However, we must make sure that the URI we submit to the HTTP call matches the
     * OAUTH_REDIRECT_URI specified in Snowflake when running CREATE SECURITY INTEGRATION...
     */
    @Config("snowflake.redirect-uri")
    @ConfigDescription("Redirect URI specified when creating the security integration")
    public SnowflakeOauthConfig setRedirectUri(String redirectUri)
    {
        this.redirectUri = redirectUri;
        return this;
    }
}
