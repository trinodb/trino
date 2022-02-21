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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.nio.file.Files.createTempFile;

public class TestStargateKerberosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StargateKerberosConfig.class)
                .setClientPrincipal(null)
                .setClientKeytabFile(null)
                .setConfigFile(null)
                .setServiceName(null)
                .setServicePrincipalPattern(null)
                .setServiceUseCanonicalHostname(true));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keytab = createTempFile(null, null);
        Path config = createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kerberos.client.principal", "principal")
                .put("kerberos.client.keytab", keytab.toString())
                .put("kerberos.config", config.toString())
                .put("kerberos.remote.service-name", "remote-service")
                .put("kerberos.service-principal-pattern", "service@host")
                .put("kerberos.service-use-canonical-hostname", "false")
                .buildOrThrow();

        StargateKerberosConfig expected = new StargateKerberosConfig()
                .setClientPrincipal("principal")
                .setClientKeytabFile(keytab.toFile())
                .setConfigFile(config.toFile())
                .setServiceName("remote-service")
                .setServicePrincipalPattern("service@host")
                .setServiceUseCanonicalHostname(false);

        assertFullMapping(properties, expected);
    }
}
