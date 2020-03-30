/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
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
                .build();

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
