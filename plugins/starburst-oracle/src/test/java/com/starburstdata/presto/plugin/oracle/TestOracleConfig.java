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

import java.math.RoundingMode;
import java.util.Map;

public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(OracleConfig.class)
                .setImpersonationEnabled(false)
                .setSynonymsEnabled(false)
                .setConnectionPoolingEnabled(true)
                .setNumberRoundingMode(RoundingMode.UNNECESSARY)
                .setDefaultNumberScale(null)
                .setAuthenticationType(OracleAuthenticationType.USER_PASSWORD));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.connection-pool.enabled", "false")
                .put("oracle.number.rounding-mode", "HALF_EVEN")
                .put("oracle.number.default-scale", "0")
                .put("oracle.authentication.type", "KERBEROS")
                .build();

        OracleConfig expected = new OracleConfig()
                .setImpersonationEnabled(true)
                .setSynonymsEnabled(true)
                .setConnectionPoolingEnabled(false)
                .setNumberRoundingMode(RoundingMode.HALF_EVEN)
                .setDefaultNumberScale(0)
                .setAuthenticationType(OracleAuthenticationType.KERBEROS);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
