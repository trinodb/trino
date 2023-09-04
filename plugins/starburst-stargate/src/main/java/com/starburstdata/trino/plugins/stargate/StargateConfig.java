/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import static com.starburstdata.trino.plugins.stargate.StargateAuthenticationType.PASSWORD;

public class StargateConfig
{
    private StargateAuthenticationType authenticationType = PASSWORD;
    private boolean impersonationEnabled;
    private boolean sslEnabled;

    public StargateAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @LegacyConfig("starburst.authentication.type")
    @Config("stargate.authentication.type")
    public StargateConfig setAuthenticationType(StargateAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @LegacyConfig("starburst.impersonation.enabled")
    @Config("stargate.impersonation.enabled")
    @ConfigDescription("Impersonate session user in remote Starburst Enterprise cluster")
    public StargateConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("ssl.enabled")
    public StargateConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }
}
