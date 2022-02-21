/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.ACTIVE_DIRECTORY_PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseConfig.SynapseAuthenticationType.PASSWORD;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSynapseConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SynapseConfig.class)
                .setImpersonationEnabled(false)
                .setAuthenticationType(PASSWORD));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("synapse.impersonation.enabled", "true")
                .put("synapse.authentication.type", "ACTIVE_DIRECTORY_PASSWORD")
                .build();

        SynapseConfig expected = new SynapseConfig()
                .setImpersonationEnabled(true)
                .setAuthenticationType(ACTIVE_DIRECTORY_PASSWORD);

        assertFullMapping(properties, expected);
    }
}
