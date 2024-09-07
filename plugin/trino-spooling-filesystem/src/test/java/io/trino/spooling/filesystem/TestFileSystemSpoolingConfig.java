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
                .setNativeAzureEnabled(false)
                .setNativeGcsEnabled(false)
                .setNativeS3Enabled(false)
                .setLocation(null)
                .setEncryptionEnabled(true)
                .setTtl(new Duration(2, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.native-azure.enabled", "true")
                .put("fs.native-gcs.enabled", "true")
                .put("fs.native-s3.enabled", "true")
                .put("location", "test")
                .put("encryption", "false")
                .put("ttl", "1h")
                .build();

        FileSystemSpoolingConfig expected = new FileSystemSpoolingConfig()
                .setNativeAzureEnabled(true)
                .setNativeGcsEnabled(true)
                .setNativeS3Enabled(true)
                .setLocation("test")
                .setEncryptionEnabled(false)
                .setTtl(new Duration(1, TimeUnit.HOURS));

        assertFullMapping(properties, expected);
    }
}
