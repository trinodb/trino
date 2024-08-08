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
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLanceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LanceConfig.class)
                .setFetchRetryCount(5)
                .setConnectionTimeout(Duration.valueOf("1m"))
                .setLanceDbUri("dummy://db.connect")
                .setConnectorType(LanceConfig.Type.DATASET.name()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        String testPath = "file://path/to/db";
        Map<String, String> properties = ImmutableMap.of(
                "lance.uri", testPath, "lance.connection-retry-count", "1",
                "lance.connection-timeout", "30s", "lance.connector-type", LanceConfig.Type.FRAGMENT.name());

        LanceConfig expected = new LanceConfig()
                .setLanceDbUri(testPath)
                .setFetchRetryCount(1)
                .setConnectionTimeout(Duration.valueOf("30s"))
                .setConnectorType(LanceConfig.Type.FRAGMENT.name());

        assertFullMapping(properties, expected);
    }
}
