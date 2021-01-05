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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestPrestoConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PrestoConnectorConfig.class)
                .setPrestoAuthenticationType(PASSWORD)
                .setImpersonationEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("starburst.authentication.type", "PASSWORD_PASS_THROUGH")
                .put("starburst.impersonation.enabled", "true")
                .build();

        PrestoConnectorConfig expected = new PrestoConnectorConfig()
                .setPrestoAuthenticationType(PASSWORD_PASS_THROUGH)
                .setImpersonationEnabled(true);

        assertFullMapping(properties, expected);
    }
}
