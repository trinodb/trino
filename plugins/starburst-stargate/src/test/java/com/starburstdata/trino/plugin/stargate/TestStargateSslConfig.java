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

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestStargateSslConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StargateSslConfig.class)
                .setTruststoreFile(null)
                .setTruststorePassword(null)
                .setTruststoreType(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ssl.truststore.path", "/dev/null")
                .put("ssl.truststore.password", "truststore-password")
                .put("ssl.truststore.type", "truststore-type")
                .build();

        StargateSslConfig expected = new StargateSslConfig()
                .setTruststoreFile(new File("/dev/null"))
                .setTruststorePassword("truststore-password")
                .setTruststoreType("truststore-type");

        assertFullMapping(properties, expected);
    }
}
