/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.NO_PARALLELISM;

public class OracleConfig
{
    private boolean impersonationEnabled;
    private boolean synonymsEnabled;
    private RoundingMode numberRoundingMode = RoundingMode.UNNECESSARY;
    private Integer defaultNumberScale;
    private boolean connectionPoolingEnabled = true;
    private OracleAuthenticationType authenticationType = PASSWORD;
    private OracleParallelismType parallelismType = NO_PARALLELISM;
    private int maxSplitsPerScan = 10; // Oracle always has a limit for number of concurrent connections

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("oracle.impersonation.enabled")
    public OracleConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    @Config("oracle.synonyms.enabled")
    public OracleConfig setSynonymsEnabled(boolean synonymsEnabled)
    {
        this.synonymsEnabled = synonymsEnabled;
        return this;
    }

    @NotNull
    public RoundingMode getNumberRoundingMode()
    {
        return numberRoundingMode;
    }

    @Config("oracle.number.rounding-mode")
    @ConfigDescription("Rounding mode for Oracle NUMBER data type")
    public OracleConfig setNumberRoundingMode(RoundingMode numberRoundingMode)
    {
        this.numberRoundingMode = numberRoundingMode;
        return this;
    }

    public Optional<@Min(0) @Max(38) Integer> getDefaultNumberScale()
    {
        return Optional.ofNullable(defaultNumberScale);
    }

    @Config("oracle.number.default-scale")
    @ConfigDescription("Default Presto DECIMAL scale for Oracle NUMBER date type")
    public OracleConfig setDefaultNumberScale(Integer defaultNumberScale)
    {
        this.defaultNumberScale = defaultNumberScale;
        return this;
    }

    public boolean isConnectionPoolingEnabled()
    {
        return connectionPoolingEnabled;
    }

    @Config("oracle.connection-pool.enabled")
    @ConfigDescription("Enables JDBC connection pooling")
    public OracleConfig setConnectionPoolingEnabled(boolean isConnectionPoolingEnabled)
    {
        this.connectionPoolingEnabled = isConnectionPoolingEnabled;
        return this;
    }

    @NotNull
    public OracleAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("oracle.authentication.type")
    @ConfigDescription("Oracle authentication mechanism type")
    public OracleConfig setAuthenticationType(OracleAuthenticationType authenticationType)
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
    public OracleConfig setParallelismType(OracleParallelismType parallelismType)
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
    public OracleConfig setMaxSplitsPerScan(int maxSplits)
    {
        this.maxSplitsPerScan = maxSplits;
        return this;
    }
}
