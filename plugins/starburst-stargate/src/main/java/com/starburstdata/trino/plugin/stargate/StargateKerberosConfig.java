/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class StargateKerberosConfig
{
    private String clientPrincipal;
    private File clientKeytabFile;
    private File configFile;
    private String serviceName;
    private Optional<String> servicePrincipalPattern = Optional.empty();
    private boolean serviceUseCanonicalHostname = true;

    @NotNull
    public String getClientPrincipal()
    {
        return clientPrincipal;
    }

    @Config("kerberos.client.principal")
    @ConfigDescription("Client principal")
    public StargateKerberosConfig setClientPrincipal(String clientPrincipal)
    {
        this.clientPrincipal = clientPrincipal;
        return this;
    }

    @NotNull
    @FileExists
    public File getClientKeytabFile()
    {
        return clientKeytabFile;
    }

    @Config("kerberos.client.keytab")
    @ConfigDescription("Client keytab location")
    public StargateKerberosConfig setClientKeytabFile(File clientKeytabFile)
    {
        this.clientKeytabFile = clientKeytabFile;
        return this;
    }

    @NotNull
    @FileExists
    public File getConfigFile()
    {
        return configFile;
    }

    @Config("kerberos.config")
    @ConfigDescription("Kerberos service configuration file")
    public StargateKerberosConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    @NotNull
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("kerberos.remote.service-name")
    @ConfigDescription("Remote peer's kerberos service name")
    public StargateKerberosConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    @NotNull
    public Optional<String> getServicePrincipalPattern()
    {
        return servicePrincipalPattern;
    }

    @Config("kerberos.service-principal-pattern")
    @ConfigDescription("Remote kerberos service principal pattern")
    public StargateKerberosConfig setServicePrincipalPattern(String servicePrincipalPattern)
    {
        this.servicePrincipalPattern = Optional.ofNullable(servicePrincipalPattern);
        return this;
    }

    public boolean isServiceUseCanonicalHostname()
    {
        return serviceUseCanonicalHostname;
    }

    @Config("kerberos.service-use-canonical-hostname")
    @ConfigDescription("Use service hostname canonicalization using the DNS reverse lookup")
    public StargateKerberosConfig setServiceUseCanonicalHostname(boolean serviceUseCanonicalHostname)
    {
        this.serviceUseCanonicalHostname = serviceUseCanonicalHostname;
        return this;
    }
}
