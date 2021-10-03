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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestImpersonationCachingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ImpersonationCachingConfig.class)
                .setUserMetastoreCacheTtl(new Duration(0, TimeUnit.SECONDS))
                .setUserMetastoreCacheMaximumSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.user-metastore-cache-ttl", "2h")
                .put("hive.user-metastore-cache-maximum-size", "5")
                .build();

        ImpersonationCachingConfig expected = new ImpersonationCachingConfig()
                .setUserMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setUserMetastoreCacheMaximumSize(5);

        assertFullMapping(properties, expected);
    }
}
