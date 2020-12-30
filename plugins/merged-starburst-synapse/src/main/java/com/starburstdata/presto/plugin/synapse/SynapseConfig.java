/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import io.airlift.configuration.Config;

public class SynapseConfig
{
    private boolean impersonationEnabled;

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("synapse.impersonation.enabled")
    public SynapseConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }
}
