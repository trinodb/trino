/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestJdbcConnectionPoolConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcConnectionPoolConfig.class)
                .setConnectionPoolEnabled(false)
                .setMaxConnectionLifetime(new Duration(30, MINUTES))
                .setMaxPoolSize(10)
                .setPoolCacheTtl(new Duration(30, MINUTES))
                .setPoolCacheMaxSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-pool.enabled", "true")
                .put("connection-pool.max-connection-lifetime", "15m")
                .put("connection-pool.max-size", "13")
                .put("connection-pool.pool-cache-ttl", "15m")
                .put("connection-pool.pool-cache-max-size", "10")
                .buildOrThrow();

        JdbcConnectionPoolConfig expected = new JdbcConnectionPoolConfig()
                .setConnectionPoolEnabled(true)
                .setMaxConnectionLifetime(new Duration(15, MINUTES))
                .setMaxPoolSize(13)
                .setPoolCacheTtl(new Duration(15, MINUTES))
                .setPoolCacheMaxSize(10);

        assertFullMapping(properties, expected);
    }
}
