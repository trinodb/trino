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
package io.trino.plugin.password.salesforce;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public class TestSalesforceConfig
{
    private final int defaultCacheSize = 4096;
    private final Duration defaultCacheExpireSeconds = Duration.succinctDuration(2, MINUTES);

    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(SalesforceConfig.class)
                .setAllowedOrganizations(null)
                .setCacheSize(defaultCacheSize)
                .setCacheExpireDuration(defaultCacheExpireSeconds));
    }

    @Test
    public void testExplicitConfig()
    {
        String org = "my18CharOrgId";
        String cacheSize = "111";
        String cacheExpire = "3333s";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("salesforce.allowed-organizations", org)
                .put("salesforce.cache-size", cacheSize)
                .put("salesforce.cache-expire-duration", cacheExpire)
                .buildOrThrow();

        SalesforceConfig expected = new SalesforceConfig()
                .setAllowedOrganizations(org)
                .setCacheSize(Integer.valueOf(cacheSize))
                .setCacheExpireDuration(Duration.valueOf(cacheExpire));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testGetOrgSet()
    {
        String orgs = "my18CharOrgId,your18CharOrgId, his18CharOrgId ,her18CharOrgId";
        int expected = (int) orgs.chars().filter(sep -> sep == ',').count() + 1;
        int actual = (new SalesforceConfig()
                .setAllowedOrganizations(orgs)).getOrgSet().size();
        assertEquals(expected, actual);
    }
}
