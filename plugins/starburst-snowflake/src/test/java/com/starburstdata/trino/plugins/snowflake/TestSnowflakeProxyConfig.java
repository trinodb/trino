/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeProxyConfig.SnowflakeProxyProtocol.HTTP;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeProxyConfig.SnowflakeProxyProtocol.HTTPS;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSnowflakeProxyConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeProxyConfig.class)
                .setProxyHost(null)
                .setProxyPort(-1)
                .setProxyProtocol(HTTP)
                .setNonProxyHosts(ImmutableList.of())
                .setUsername(null)
                .setPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.proxy.host", "example.com")
                .put("snowflake.proxy.port", "80")
                .put("snowflake.proxy.protocol", "HTTPS")
                .put("snowflake.proxy.non-proxy-hosts", "*.snowflakecomputing.com,s3.amazonaws.com,192.168.*")
                .put("snowflake.proxy.username", "foo")
                .put("snowflake.proxy.password", "bar")
                .buildOrThrow();

        SnowflakeProxyConfig expected = new SnowflakeProxyConfig()
                .setProxyHost("example.com")
                .setProxyPort(80)
                .setProxyProtocol(HTTPS)
                .setNonProxyHosts(ImmutableList.of("*.snowflakecomputing.com", "s3.amazonaws.com", "192.168.*"))
                .setUsername("foo")
                .setPassword("bar");

        assertFullMapping(properties, expected);
    }
}
