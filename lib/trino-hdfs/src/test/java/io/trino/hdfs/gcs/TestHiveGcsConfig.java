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
package io.trino.hdfs.gcs;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveGcsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveGcsConfig.class)
                .setUseGcsAccessToken(false)
                .setJsonKey(null)
                .setJsonKeyFilePath(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path jsonKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.gcs.use-access-token", "true")
                .put("hive.gcs.json-key", "{}")
                .put("hive.gcs.json-key-file-path", jsonKeyFile.toString())
                .buildOrThrow();

        HiveGcsConfig expected = new HiveGcsConfig()
                .setUseGcsAccessToken(true)
                .setJsonKey("{}")
                .setJsonKeyFilePath(jsonKeyFile.toString());

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(
                new HiveGcsConfig()
                        .setUseGcsAccessToken(true)
                        .setJsonKey("{}}")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'hive.gcs.json-key' when 'hive.gcs.use-access-token' is set");

        assertThatThrownBy(
                new HiveGcsConfig()
                        .setUseGcsAccessToken(true)
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'hive.gcs.json-key-file-path' when 'hive.gcs.use-access-token' is set");

        assertThatThrownBy(
                new HiveGcsConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("'hive.gcs.json-key' and 'hive.gcs.json-key-file-path' cannot be both set");
    }
}
