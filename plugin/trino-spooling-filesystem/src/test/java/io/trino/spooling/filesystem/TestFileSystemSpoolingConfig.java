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
package io.trino.spooling.filesystem;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestFileSystemSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileSystemSpoolingConfig.class)
                .setAzureEnabled(false)
                .setGcsEnabled(false)
                .setS3Enabled(false)
                .setLocation(null)
                .setEncryptionEnabled(true)
                .setTtl(new Duration(12, TimeUnit.HOURS))
                .setPruningEnabled(true)
                .setPruningInterval(new Duration(5, TimeUnit.MINUTES))
                .setPruningBatchSize(250));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.azure.enabled", "true")
                .put("fs.gcs.enabled", "true")
                .put("fs.s3.enabled", "true")
                .put("fs.location", "test")
                .put("fs.segment.encryption", "false")
                .put("fs.segment.ttl", "1h")
                .put("fs.segment.pruning.enabled", "false")
                .put("fs.segment.pruning.interval", "12h")
                .put("fs.segment.pruning.batch-size", "5")
                .buildOrThrow();

        FileSystemSpoolingConfig expected = new FileSystemSpoolingConfig()
                .setAzureEnabled(true)
                .setGcsEnabled(true)
                .setS3Enabled(true)
                .setLocation("test")
                .setEncryptionEnabled(false)
                .setTtl(new Duration(1, TimeUnit.HOURS))
                .setPruningEnabled(false)
                .setPruningInterval(new Duration(12, TimeUnit.HOURS))
                .setPruningBatchSize(5);

        assertFullMapping(properties, expected);
    }
}
