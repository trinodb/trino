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

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class SalesforcePasswordConfig
{
    private String user;
    private String password;
    private String securityToken;

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("salesforce.user")
    @ConfigDescription("Username used to log in to Salesforce")
    public SalesforcePasswordConfig setUser(String user)
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
    public SalesforcePasswordConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getSecurityToken()
    {
        return Optional.ofNullable(securityToken);
    }

    @Config("salesforce.security-token")
    @ConfigDescription("Salesforce Security Token for the configured username for programmatic access")
    @ConfigSecuritySensitive
    public SalesforcePasswordConfig setSecurityToken(String securityToken)
    {
        this.securityToken = securityToken;
        return this;
    }
}
