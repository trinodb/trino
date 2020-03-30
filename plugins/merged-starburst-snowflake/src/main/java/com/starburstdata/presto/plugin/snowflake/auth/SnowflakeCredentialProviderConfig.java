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

public class SnowflakeCredentialProviderConfig
{
    private boolean useExtraCredentials;

    public boolean isUseExtraCredentials()
    {
        return useExtraCredentials;
    }

    @Config("snowflake.credential.use-extra-credentials")
    @ConfigDescription("Enables the use of extra credentials")
    public void setUseExtraCredentials(boolean useExtraCredentials)
    {
        this.useExtraCredentials = useExtraCredentials;
    }
}
