/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.saphana.SapHanaAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.saphana.SapHanaAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSapHanaAuthenticationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SapHanaAuthenticationConfig.class)
                .setAuthenticationType(PASSWORD));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sap-hana.authentication.type", "PASSWORD_PASS_THROUGH")
                .buildOrThrow();

        SapHanaAuthenticationConfig expected = new SapHanaAuthenticationConfig()
                .setAuthenticationType(PASSWORD_PASS_THROUGH);

        assertFullMapping(properties, expected);
    }
}
