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
package io.trino.filesystem.azure;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.filesystem.azure.AzureFileSystemConfig.AuthType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestAzureFileSystemConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AzureFileSystemConfig.class)
                .setAuthType(AuthType.NONE)
                .setReadBlockSize(DataSize.of(4, Unit.MEGABYTE))
                .setWriteBlockSize(DataSize.of(4, Unit.MEGABYTE))
                .setMaxWriteConcurrency(8)
                .setMaxSingleUploadSize(DataSize.of(4, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("azure.auth-type", "oauth")
                .put("azure.read-block-size", "3MB")
                .put("azure.write-block-size", "5MB")
                .put("azure.max-write-concurrency", "7")
                .put("azure.max-single-upload-size", "7MB")
                .buildOrThrow();

        AzureFileSystemConfig expected = new AzureFileSystemConfig()
                .setAuthType(AuthType.OAUTH)
                .setReadBlockSize(DataSize.of(3, Unit.MEGABYTE))
                .setWriteBlockSize(DataSize.of(5, Unit.MEGABYTE))
                .setMaxWriteConcurrency(7)
                .setMaxSingleUploadSize(DataSize.of(7, Unit.MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
