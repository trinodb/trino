/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.starburstdata.trino.plugins.oracle.OracleAuthenticationType.PASSWORD;
import static com.starburstdata.trino.plugins.oracle.OracleParallelismType.NO_PARALLELISM;

public class StarburstOracleConfig
{
    private boolean impersonationEnabled;
    private OracleAuthenticationType authenticationType = PASSWORD;
    private OracleParallelismType parallelismType = NO_PARALLELISM;
    private int maxSplitsPerScan = 10; // Oracle always has a limit for number of concurrent connections

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("oracle.impersonation.enabled")
    public StarburstOracleConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    @NotNull
    public OracleAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("oracle.authentication.type")
    @ConfigDescription("Oracle authentication mechanism type")
    public StarburstOracleConfig setAuthenticationType(OracleAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    @NotNull
    public OracleParallelismType getParallelismType()
    {
        return parallelismType;
    }

    @Config("oracle.parallelism-type")
    @LegacyConfig("oracle.concurrency-type")
    @ConfigDescription("Concurrency strategy for reads")
    public StarburstOracleConfig setParallelismType(OracleParallelismType parallelismType)
    {
        this.parallelismType = parallelismType;
        return this;
    }

    @Min(1)
    public int getMaxSplitsPerScan()
    {
        return maxSplitsPerScan;
    }

    @LegacyConfig("oracle.concurrent.max-splits-per-scan")
    @Config("oracle.parallel.max-splits-per-scan")
    @ConfigDescription("Maximum number of splits for a table scan")
    public StarburstOracleConfig setMaxSplitsPerScan(int maxSplits)
    {
        this.maxSplitsPerScan = maxSplits;
        return this;
    }
}
