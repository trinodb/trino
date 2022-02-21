/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlServerTlsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SqlServerTlsConfig.class)
                .setTruststoreFile(null)
                .setTruststorePassword(null)
                .setTruststoreType("JKS"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.tls.truststore-path", "/dev/null")
                .put("sqlserver.tls.truststore-password", "password")
                .put("sqlserver.tls.truststore-type", "PKCS12")
                .buildOrThrow();

        SqlServerTlsConfig expected = new SqlServerTlsConfig()
                .setTruststoreFile(new File("/dev/null"))
                .setTruststorePassword("password")
                .setTruststoreType("PKCS12");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsFailsIfTruststoreDoesNotExist()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.tls.truststore-path", "/non/existent/file/path")
                .put("sqlserver.tls.truststore-password", "password")
                .put("sqlserver.tls.truststore-type", "JKS")
                .buildOrThrow();

        SqlServerTlsConfig expected = new SqlServerTlsConfig();

        assertThatThrownBy(() -> assertFullMapping(properties, expected))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("file does not exist");
    }

    @Test
    public void testInvalidConfiguration()
    {
        assertThatThrownBy(() -> new SqlServerTlsConfig()
                .setTruststorePassword("12345")
                .validate())
                .hasMessageContaining("sqlserver.tls.truststore-password needs to be set in order to use sqlserver.tls.truststore-path parameter");

        assertThatThrownBy(() -> new SqlServerTlsConfig()
                .setTruststoreFile(new File("/tmp/test"))
                .validate())
                .hasMessageContaining("sqlserver.tls.truststore-password needs to be set in order to use sqlserver.tls.truststore-path parameter");
    }
}
