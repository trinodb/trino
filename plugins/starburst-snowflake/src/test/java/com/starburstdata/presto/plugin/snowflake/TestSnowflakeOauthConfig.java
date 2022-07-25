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
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthConfig;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestSnowflakeOauthConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeOauthConfig.class)
                .setCredentialCacheSize(10000)
                .setHttpConnectTimeout(new Duration(30, SECONDS))
                .setHttpReadTimeout(new Duration(30, SECONDS))
                .setHttpWriteTimeout(new Duration(30, SECONDS))
                .setCredentialTtl(null)
                .setRedirectUri("https://localhost")
                .setAccountUrl(null)
                .setAccountName(null)
                .setClientId(null)
                .setClientSecret(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.credential.cache-size", "2000")
                .put("snowflake.credential.http-connect-timeout", "1m")
                .put("snowflake.credential.http-read-timeout", "1m")
                .put("snowflake.credential.http-write-timeout", "1m")
                .put("snowflake.credential.cache-ttl", "12h")
                .put("snowflake.redirect-uri", "https://something.com")
                .put("snowflake.account-url", "https://LJ06752.us-east-2.aws.snowflakecomputing.com")
                .put("snowflake.account-name", "LJ06752")
                .put("snowflake.client-id", "U9AZUFx3G4kGZwTzKTBEzTkRBwQ=")
                .put("snowflake.client-secret", "XXX")
                .buildOrThrow();

        SnowflakeOauthConfig expected = new SnowflakeOauthConfig()
                .setCredentialCacheSize(2000)
                .setHttpConnectTimeout(new Duration(1, MINUTES))
                .setHttpReadTimeout(new Duration(1, MINUTES))
                .setHttpWriteTimeout(new Duration(1, MINUTES))
                .setCredentialTtl(new Duration(12, HOURS))
                .setRedirectUri("https://something.com")
                .setAccountUrl("https://LJ06752.us-east-2.aws.snowflakecomputing.com")
                .setAccountName("LJ06752")
                .setClientId("U9AZUFx3G4kGZwTzKTBEzTkRBwQ=")
                .setClientSecret("XXX");

        assertFullMapping(properties, expected);
    }
}
