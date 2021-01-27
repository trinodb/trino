/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class PrestoConnectorSslConfig
{
    private File truststoreFile;
    private String truststorePassword;
    private String truststoreType;

    @NotNull
    public Optional<@FileExists File> getTruststoreFile()
    {
        return Optional.ofNullable(truststoreFile);
    }

    @Config("ssl.truststore.path")
    @ConfigDescription("File path to the location of the SSL truststore to use")
    public PrestoConnectorSslConfig setTruststoreFile(File truststoreFile)
    {
        this.truststoreFile = truststoreFile;
        return this;
    }

    @NotNull
    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("ssl.truststore.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the truststore")
    public PrestoConnectorSslConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @NotNull
    public Optional<String> getTruststoreType()
    {
        return Optional.ofNullable(truststoreType);
    }

    @Config("ssl.truststore.type")
    @ConfigDescription("Type of truststore file used")
    public PrestoConnectorSslConfig setTruststoreType(String truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }
}
