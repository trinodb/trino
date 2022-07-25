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
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class OktaConfig
{
    private String accountUrl;
    private Duration httpConnectTimeout = new Duration(30, SECONDS);
    private Duration httpReadTimeout = new Duration(30, SECONDS);
    private Duration httpWriteTimeout = new Duration(30, SECONDS);

    public Duration getHttpConnectTimeout()
    {
        return httpConnectTimeout;
    }

    @Config("okta.http-connect-timeout")
    @ConfigDescription("Connect timeout for Okta HTTP calls")
    public OktaConfig setHttpConnectTimeout(Duration httpConnectTimeout)
    {
        this.httpConnectTimeout = httpConnectTimeout;
        return this;
    }

    public Duration getHttpReadTimeout()
    {
        return httpReadTimeout;
    }

    @Config("okta.http-read-timeout")
    @ConfigDescription("Read timeout for Okta HTTP calls")
    public OktaConfig setHttpReadTimeout(Duration httpReadTimeout)
    {
        this.httpReadTimeout = httpReadTimeout;
        return this;
    }

    public Duration getHttpWriteTimeout()
    {
        return httpWriteTimeout;
    }

    @Config("okta.http-write-timeout")
    @ConfigDescription("Write timeout for Okta HTTP calls")
    public OktaConfig setHttpWriteTimeout(Duration httpWriteTimeout)
    {
        this.httpWriteTimeout = httpWriteTimeout;
        return this;
    }

    @NotNull
    public String getAccountUrl()
    {
        return accountUrl;
    }

    @Config("okta.account-url")
    @ConfigDescription("Okta URL (https://your_okta_account_name.okta.com)")
    public OktaConfig setAccountUrl(String accountUrl)
    {
        this.accountUrl = accountUrl;
        return this;
    }
}
