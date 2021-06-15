/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import static com.starburstdata.trino.plugin.stargate.StarburstRemoteAuthenticationType.PASSWORD;

public class StarburstRemoteConfig
{
    private StarburstRemoteAuthenticationType authenticationType = PASSWORD;
    private boolean impersonationEnabled;
    private boolean sslEnabled;

    public StarburstRemoteAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("starburst.authentication.type")
    public StarburstRemoteConfig setAuthenticationType(StarburstRemoteAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("starburst.impersonation.enabled")
    @ConfigDescription("Impersonate session user in remote Starburst Enterprise cluster")
    public StarburstRemoteConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("ssl.enabled")
    public StarburstRemoteConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }
}
