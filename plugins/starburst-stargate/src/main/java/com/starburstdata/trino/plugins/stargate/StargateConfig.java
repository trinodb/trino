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

public class StargateConfig
{
    private boolean sslEnabled;

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
