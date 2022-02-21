/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.NO_PARALLELISM;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.PARTITIONS;
import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

public class TestStarburstOracleConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(StarburstOracleConfig.class)
                .setImpersonationEnabled(false)
                .setParallelismType(NO_PARALLELISM)
                .setAuthenticationType(OracleAuthenticationType.PASSWORD)
                .setMaxSplitsPerScan(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.parallelism-type", "PARTITIONS")
                .put("oracle.parallel.max-splits-per-scan", "42")
                .build();

        StarburstOracleConfig expected = new StarburstOracleConfig()
                .setAuthenticationType(OracleAuthenticationType.KERBEROS)
                .setParallelismType(PARTITIONS)
                .setMaxSplitsPerScan(42)
                .setImpersonationEnabled(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsForLegacyValues()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.parallelism-type", "PARTITIONS")
                .put("oracle.parallel.max-splits-per-scan", "42")
                .build();

        StarburstOracleConfig expected = new StarburstOracleConfig()
                .setImpersonationEnabled(true)
                .setAuthenticationType(OracleAuthenticationType.KERBEROS)
                .setParallelismType(PARTITIONS)
                .setMaxSplitsPerScan(42);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyPropertyMappings()
    {
        Map<String, String> oldProperties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.concurrency-type", "PARTITIONS")
                .put("oracle.concurrent.max-splits-per-scan", "42")
                .build();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.parallelism-type", "PARTITIONS")
                .put("oracle.parallel.max-splits-per-scan", "42")
                .build();

        assertDeprecatedEquivalence(StarburstOracleConfig.class, properties, oldProperties);
    }

    @Test
    public void testLegacyPropertyMappings2()
    {
        Map<String, String> oldProperties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.concurrency-type", "NO_CONCURRENCY")
                .put("oracle.concurrent.max-splits-per-scan", "42")
                .build();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("oracle.parallelism-type", "NO_PARALLELISM")
                .put("oracle.parallel.max-splits-per-scan", "42")
                .build();

        assertDeprecatedEquivalence(StarburstOracleConfig.class, properties, oldProperties);
    }
}
