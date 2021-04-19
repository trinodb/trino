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
import io.airlift.configuration.ConfigDescription;

import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD;

public class SynapseConfig
{
    private boolean impersonationEnabled;
    private SynapseAuthenticationType authenticationType = PASSWORD;

    public enum SynapseAuthenticationType
    {
        ACTIVE_DIRECTORY_PASSWORD,
        PASSWORD,
        ACTIVE_DIRECTORY_PASSWORD_PASS_THROUGH,
        PASSWORD_PASS_THROUGH,
    }

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

    public SynapseAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("synapse.authentication.type")
    @ConfigDescription("Synapse authentication mechanism")
    public SynapseConfig setAuthenticationType(SynapseAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }
}
