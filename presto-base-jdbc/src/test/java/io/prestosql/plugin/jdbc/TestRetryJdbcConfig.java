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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRetryJdbcConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RetryJdbcConfig.class)
                .setDelay(Duration.valueOf("1s"))
                .setMaxDelay(Duration.valueOf("1m"))
                .setMaxRetries(10)
                .setDelayFactor(2.0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connect.retry-delay", "2s")
                .put("connect.max-retry-delay", "5m")
                .put("connect.max-retries", "20")
                .put("connect.retry-delay-factor", "1.5")
                .build();

        RetryJdbcConfig expected = new RetryJdbcConfig()
                .setDelay(Duration.valueOf("2s"))
                .setMaxDelay(Duration.valueOf("5m"))
                .setMaxRetries(20)
                .setDelayFactor(1.5);

        assertFullMapping(properties, expected);
    }
}
