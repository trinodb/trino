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
import io.airlift.configuration.LegacyConfig;

public class StargateConfig
{
    public static final String PASSWORD = "PASSWORD";

    private String authenticationType = PASSWORD;
    private boolean sslEnabled;

    public String getAuthenticationType()
    {
        return authenticationType;
    }

    @LegacyConfig("starburst.authentication.type")
    @Config("stargate.authentication.type")
    public StargateConfig setAuthenticationType(String authenticationType)
    {
        this.authenticationType = authenticationType;
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
