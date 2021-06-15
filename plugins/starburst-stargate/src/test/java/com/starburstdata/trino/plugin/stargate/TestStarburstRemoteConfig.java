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

import java.util.Map;

import static com.starburstdata.trino.plugin.stargate.StarburstRemoteAuthenticationType.PASSWORD;
import static com.starburstdata.trino.plugin.stargate.StarburstRemoteAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestStarburstRemoteConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarburstRemoteConfig.class)
                .setAuthenticationType(PASSWORD)
                .setImpersonationEnabled(false)
                .setSslEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("starburst.authentication.type", "PASSWORD_PASS_THROUGH")
                .put("starburst.impersonation.enabled", "true")
                .put("ssl.enabled", "true")
                .build();

        StarburstRemoteConfig expected = new StarburstRemoteConfig()
                .setAuthenticationType(PASSWORD_PASS_THROUGH)
                .setImpersonationEnabled(true)
                .setSslEnabled(true);

        assertFullMapping(properties, expected);
    }
}
