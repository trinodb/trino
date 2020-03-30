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
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
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
