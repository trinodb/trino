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
package io.trino.filesystem.gcs;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGcsFileSystemConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsFileSystemConfig.class)
                .setReadBlockSize(DataSize.of(2, DataSize.Unit.MEGABYTE))
                .setWriteBlockSize(DataSize.of(16, DataSize.Unit.MEGABYTE))
                .setPageSize(100)
                .setBatchSize(100)
                .setProjectId(null)
                .setUseGcsAccessToken(false)
                .setJsonKey(null)
                .setJsonKeyFilePath(null));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path jsonKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcs.read-block-size", "51MB")
                .put("gcs.write-block-size", "52MB")
                .put("gcs.page-size", "10")
                .put("gcs.batch-size", "11")
                .put("gcs.projectId", "project")
                .put("gcs.use-access-token", "true")
                .put("gcs.json-key", "{}")
                .put("gcs.json-key-file-path", jsonKeyFile.toString())
                .buildOrThrow();

        GcsFileSystemConfig expected = new GcsFileSystemConfig()
                .setReadBlockSize(DataSize.of(51, DataSize.Unit.MEGABYTE))
                .setWriteBlockSize(DataSize.of(52, DataSize.Unit.MEGABYTE))
                .setPageSize(10)
                .setBatchSize(11)
                .setProjectId("project")
                .setUseGcsAccessToken(true)
                .setJsonKey("{}")
                .setJsonKeyFilePath(jsonKeyFile.toString());
        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(true)
                        .setJsonKey("{}}")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'hive.gcs.json-key' when 'hive.gcs.use-access-token' is set");

        assertThatThrownBy(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(true)
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'hive.gcs.json-key-file-path' when 'hive.gcs.use-access-token' is set");

        assertThatThrownBy(
                new GcsFileSystemConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("'hive.gcs.json-key' and 'hive.gcs.json-key-file-path' cannot be both set");
    }
}
