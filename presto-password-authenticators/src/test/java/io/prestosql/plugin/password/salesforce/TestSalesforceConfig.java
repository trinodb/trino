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
package io.prestosql.plugin.password.salesforce;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestSalesforceConfig
{
    private final int defaultCacheSize = 4096;
    private final int defaultCacheExpireSeconds = 120;

    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(SalesforceConfig.class)
                .setOrgs(null)
                .setCacheSize(defaultCacheSize)
                .setCacheExpireSeconds(defaultCacheExpireSeconds));
    }

    // Test will only pass if config is created with properties that are different than the defaults.
    @Test
    public void testExplicitConfig()
    {
        String org = "my18CharOrgId";
        String cacheSize = "128";
        String cacheExpire = "3600";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("salesforce.org", org)
                .put("salesforce.cache-size", cacheSize)
                .put("salesforce.cache-expire-seconds", cacheExpire)
                .build();

        SalesforceConfig expected = new SalesforceConfig()
                .setOrgs(org)
                .setCacheSize(Integer.valueOf(cacheSize))
                .setCacheExpireSeconds(Integer.valueOf(cacheExpire));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testGetOrgSet()
    {
        String orgs = "my18CharOrgId,your18CharOrgId, his18CharOrgId ,her18CharOrgId";
        int expected = (int) orgs.chars().filter(sep -> sep == ',').count() + 1;
        int actual = (new SalesforceConfig()
                .setOrgs(orgs)).getOrgSet().size();
        assertEquals(expected, actual);
    }
}
