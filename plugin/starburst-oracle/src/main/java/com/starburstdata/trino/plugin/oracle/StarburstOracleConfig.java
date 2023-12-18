/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static com.starburstdata.trino.plugin.oracle.OracleParallelismType.NO_PARALLELISM;

public class StarburstOracleConfig
{
    public static final String PASSWORD = "PASSWORD";

    private OracleParallelismType parallelismType = NO_PARALLELISM;
    private String authenticationType = PASSWORD;
    private int maxSplitsPerScan = 10; // Oracle always has a limit for number of concurrent connections

    @NotNull
    public String getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("oracle.authentication.type")
    @ConfigDescription("Oracle authentication mechanism type")
    public StarburstOracleConfig setAuthenticationType(String authenticationType)
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
