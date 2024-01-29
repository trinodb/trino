/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class SqlServerTlsConfig
{
    private static final String SQLSERVER_TLS_TRUSTSTORE = "sqlserver.tls.truststore-path";
    private static final String SQLSERVER_TLS_TRUSTSTORE_PASSWORD = "sqlserver.tls.truststore-password";

    private File truststoreFile;
    private String truststorePassword;
    private String truststoreType = "JKS";

    @NotNull
    public Optional<@FileExists File> getTruststoreFile()
    {
        return Optional.ofNullable(truststoreFile);
    }

    @Config(SQLSERVER_TLS_TRUSTSTORE)
    @ConfigDescription("The location of the truststore file for TLS; Highly suggested to use TLS with NTLM auth")
    public SqlServerTlsConfig setTruststoreFile(File truststoreFile)
    {
        this.truststoreFile = truststoreFile;
        return this;
    }

    @NotNull
    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config(SQLSERVER_TLS_TRUSTSTORE_PASSWORD)
    @ConfigSecuritySensitive
    @ConfigDescription("The password used to encrypt the truststore file for TLS")
    public SqlServerTlsConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @NotNull
    public String getTruststoreType()
    {
        return truststoreType;
    }

    @Config("sqlserver.tls.truststore-type")
    @ConfigDescription("The type of truststore file used")
    public SqlServerTlsConfig setTruststoreType(String truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        verify(
                (truststoreFile == null && truststorePassword == null) || (truststoreFile != null && truststorePassword != null),
                "%s needs to be set in order to use %s parameter", SQLSERVER_TLS_TRUSTSTORE_PASSWORD, SQLSERVER_TLS_TRUSTSTORE);
    }
}
