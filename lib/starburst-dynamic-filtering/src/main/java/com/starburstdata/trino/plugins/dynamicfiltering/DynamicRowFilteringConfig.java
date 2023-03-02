/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DynamicRowFilteringConfig
{
    private boolean dynamicRowFilteringEnabled = true;
    private double dynamicRowFilterSelectivityThreshold = 0.7;
    private Duration dynamicRowFilteringBlockingTimeout = new Duration(0, SECONDS);

    public boolean isDynamicRowFilteringEnabled()
    {
        return dynamicRowFilteringEnabled;
    }

    @Config("dynamic-row-filtering.enabled")
    @ConfigDescription("Enable dynamic row filtering")
    public DynamicRowFilteringConfig setDynamicRowFilteringEnabled(boolean dynamicRowFilteringEnabled)
    {
        this.dynamicRowFilteringEnabled = dynamicRowFilteringEnabled;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getDynamicRowFilterSelectivityThreshold()
    {
        return dynamicRowFilterSelectivityThreshold;
    }

    @Config("dynamic-row-filtering.selectivity-threshold")
    @ConfigDescription("Avoid using dynamic row filters when fraction of rows selected is above threshold")
    public DynamicRowFilteringConfig setDynamicRowFilterSelectivityThreshold(double dynamicRowFilterSelectivityThreshold)
    {
        this.dynamicRowFilterSelectivityThreshold = dynamicRowFilterSelectivityThreshold;
        return this;
    }

    @NotNull
    public Duration getDynamicRowFilteringWaitTimeout()
    {
        return dynamicRowFilteringBlockingTimeout;
    }

    @Config("dynamic-row-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public DynamicRowFilteringConfig setDynamicRowFilteringWaitTimeout(Duration dynamicRowFilteringBlockingTimeout)
    {
        this.dynamicRowFilteringBlockingTimeout = dynamicRowFilteringBlockingTimeout;
        return this;
    }
}
