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
package io.trino.spiller;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestNodeSpillConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NodeSpillConfig.class)
                .setMaxSpillPerNode(DataSize.of(100, GIGABYTE))
                .setQueryMaxSpillPerNode(DataSize.of(100, GIGABYTE))
                .setSpillCompressionEnabled(false)
                .setSpillEncryptionEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("max-spill-per-node", "10MB")
                .put("query-max-spill-per-node", "15 MB")
                .put("spill-compression-enabled", "true")
                .put("spill-encryption-enabled", "true")
                .buildOrThrow();

        NodeSpillConfig expected = new NodeSpillConfig()
                .setMaxSpillPerNode(DataSize.of(10, MEGABYTE))
                .setQueryMaxSpillPerNode(DataSize.of(15, MEGABYTE))
                .setSpillCompressionEnabled(true)
                .setSpillEncryptionEnabled(true);

        assertFullMapping(properties, expected);
    }
}
