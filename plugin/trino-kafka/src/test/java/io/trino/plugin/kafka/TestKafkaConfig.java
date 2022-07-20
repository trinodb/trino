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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kafka.schema.file.FileTableDescriptionSupplier;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestKafkaConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaConfig.class)
                .setNodes("")
                .setKafkaBufferSize("64kB")
                .setDefaultSchema("default")
                .setTableDescriptionSupplier(FileTableDescriptionSupplier.NAME)
                .setHideInternalColumns(true)
                .setMessagesPerSplit(100_000)
                .setTimestampUpperBoundPushDownEnabled(false)
                .setResourceConfigFiles(List.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path resource1 = Files.createTempFile(null, null);
        Path resource2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.default-schema", "kafka")
                .put("kafka.table-description-supplier", "test")
                .put("kafka.nodes", "localhost:12345,localhost:23456")
                .put("kafka.buffer-size", "1MB")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.messages-per-split", "1")
                .put("kafka.timestamp-upper-bound-force-push-down-enabled", "true")
                .put("kafka.config.resources", resource1.toString() + "," + resource2.toString())
                .buildOrThrow();

        KafkaConfig expected = new KafkaConfig()
                .setDefaultSchema("kafka")
                .setTableDescriptionSupplier("test")
                .setNodes("localhost:12345, localhost:23456")
                .setKafkaBufferSize("1MB")
                .setHideInternalColumns(false)
                .setMessagesPerSplit(1)
                .setTimestampUpperBoundPushDownEnabled(true)
                .setResourceConfigFiles(ImmutableList.of(resource1.toString(), resource2.toString()));

        assertFullMapping(properties, expected);
    }
}
