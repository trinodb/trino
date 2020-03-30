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
