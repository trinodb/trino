/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.snowflake.auth.OktaConfig;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOktaConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OktaConfig.class)
                .setHttpConnectTimeout(new Duration(30, SECONDS))
                .setHttpReadTimeout(new Duration(30, SECONDS))
                .setHttpWriteTimeout(new Duration(30, SECONDS))
                .setAccountUrl(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("okta.http-connect-timeout", "1m")
                .put("okta.http-read-timeout", "1m")
                .put("okta.http-write-timeout", "1m")
                .put("okta.account-url", "https://dev-966531.okta.com")
                .build();

        OktaConfig expected = new OktaConfig()
                .setHttpConnectTimeout(new Duration(1, MINUTES))
                .setHttpReadTimeout(new Duration(1, MINUTES))
                .setHttpWriteTimeout(new Duration(1, MINUTES))
                .setAccountUrl("https://dev-966531.okta.com");

        assertFullMapping(properties, expected);
    }
}
