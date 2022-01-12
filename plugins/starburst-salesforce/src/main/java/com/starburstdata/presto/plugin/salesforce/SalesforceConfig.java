/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SalesforceAuthenticationType.PASSWORD;

public class SalesforceConfig
{
    public enum SalesforceAuthenticationType
    {
        PASSWORD,
        OAUTH_JWT,
    }

    private SalesforceAuthenticationType authenticationType = PASSWORD;
    private boolean isSandboxEnabled;
    private boolean isDriverLoggingEnabled;
    private String driverLoggingLocation = System.getProperty("java.io.tmpdir") + "/salesforce.log";
    private int driverLoggingVerbosity = 3;
    private Optional<String> extraJdbcProperties = Optional.empty();

    @NotNull
    public SalesforceAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("salesforce.authentication.type")
    @ConfigDescription("Method of authenticating with Salesforce. Default is PASSWORD")
    public SalesforceConfig setAuthenticationType(SalesforceAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public boolean isSandboxEnabled()
    {
        return isSandboxEnabled;
    }

    @Config("salesforce.enable-sandbox")
    @ConfigDescription("Set to true to enable using the Salesforce Sandbox, false otherwise. Default is false")
    public SalesforceConfig setSandboxEnabled(boolean isSandboxEnabled)
    {
        this.isSandboxEnabled = isSandboxEnabled;
        return this;
    }

    public boolean isDriverLoggingEnabled()
    {
        return isDriverLoggingEnabled;
    }

    @Config("salesforce.driver-logging.enabled")
    @ConfigDescription("True to enable the logging feature on the driver. Default is false")
    public SalesforceConfig setDriverLoggingEnabled(boolean isDriverLoggingEnabled)
    {
        this.isDriverLoggingEnabled = isDriverLoggingEnabled;
        return this;
    }

    public String getDriverLoggingLocation()
    {
        return driverLoggingLocation;
    }

    @Config("salesforce.driver-logging.location")
    @ConfigDescription("Sets the name of the file for to put driver logs. Default is ${java.io.tmpdir}/salesforce.log")
    public SalesforceConfig setDriverLoggingLocation(String driverLoggingLocation)
    {
        this.driverLoggingLocation = driverLoggingLocation;
        return this;
    }

    @Min(1)
    @Max(5)
    public int getDriverLoggingVerbosity()
    {
        return driverLoggingVerbosity;
    }

    @Config("salesforce.driver-logging.verbosity")
    @ConfigDescription("Sets the logging verbosity level. Default level 3")
    public SalesforceConfig setDriverLoggingVerbosity(int driverLoggingVerbosity)
    {
        this.driverLoggingVerbosity = driverLoggingVerbosity;
        return this;
    }

    public Optional<String> getExtraJdbcProperties()
    {
        return extraJdbcProperties;
    }

    @Config("salesforce.extra-jdbc-properties")
    @ConfigDescription("Any extra properties to add to the JDBC Driver connection")
    public SalesforceConfig setExtraJdbcProperties(String extraJdbcProperties)
    {
        this.extraJdbcProperties = Optional.ofNullable(extraJdbcProperties);
        return this;
    }
}
