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
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class SalesforceConfig
{
    // Temporary hack until license management system can support CData use case
    // Users will have to enter this value for "salesforce.key" in the connector configuration
    // TODO https://starburstdata.atlassian.net/browse/SEP-5892
    public static final String SALESFORCE_CONNECTOR_KEY_VALUE = "Cnvui77iTrjMqy_FqW~j";

    private String key;
    private String user;
    private String password;
    private String securityToken;
    private boolean isSandboxEnabled;
    private boolean isDriverLoggingEnabled;
    private String driverLoggingLocation = System.getProperty("java.io.tmpdir") + "/salesforce.log";
    private int driverLoggingVerbosity = 3;
    private Optional<String> extraJdbcProperties = Optional.empty();

    @NotNull
    public String getKey()
    {
        return key;
    }

    @Config("salesforce.key")
    @ConfigDescription("Key to unlock the Salesforce connector")
    public SalesforceConfig setKey(String key)
    {
        this.key = key;
        return this;
    }

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("salesforce.user")
    @ConfigDescription("Username used to log in to Salesforce")
    public SalesforceConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("salesforce.password")
    @ConfigDescription("Password used to log in to Salesforce")
    @ConfigSecuritySensitive
    public SalesforceConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public String getSecurityToken()
    {
        return securityToken;
    }

    @Config("salesforce.security-token")
    @ConfigDescription("Salesforce Security Token for the configured username for programmatic access")
    @ConfigSecuritySensitive
    public SalesforceConfig setSecurityToken(String securityToken)
    {
        this.securityToken = securityToken;
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

    @PostConstruct
    public void validate()
    {
        checkState(getKey() != null && getKey().equals(SALESFORCE_CONNECTOR_KEY_VALUE),
                "Users must specify a correct value for 'salesforce.key' in the connector config to use the Salesforce connector. Contact Starburst Support for details.");
    }
}
