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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveFormatsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveFormatsConfig.class)
                .setAvroFileNativeReaderEnabled(false)
                .setAvroFileNativeWriterEnabled(false)
                .setCsvNativeReaderEnabled(false)
                .setCsvNativeWriterEnabled(false)
                .setJsonNativeReaderEnabled(false)
                .setJsonNativeWriterEnabled(false)
                .setOpenXJsonNativeReaderEnabled(false)
                .setOpenXJsonNativeWriterEnabled(false)
                .setRegexNativeReaderEnabled(false)
                .setTextFileNativeReaderEnabled(false)
                .setTextFileNativeWriterEnabled(false)
                .setSequenceFileNativeReaderEnabled(false)
                .setSequenceFileNativeWriterEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("avro.native-reader.enabled", "true")
                .put("avro.native-writer.enabled", "true")
                .put("csv.native-reader.enabled", "true")
                .put("csv.native-writer.enabled", "true")
                .put("json.native-reader.enabled", "true")
                .put("json.native-writer.enabled", "true")
                .put("openx-json.native-reader.enabled", "true")
                .put("openx-json.native-writer.enabled", "true")
                .put("regex.native-reader.enabled", "true")
                .put("text-file.native-reader.enabled", "true")
                .put("text-file.native-writer.enabled", "true")
                .put("sequence-file.native-reader.enabled", "true")
                .put("sequence-file.native-writer.enabled", "true")
                .buildOrThrow();

        HiveFormatsConfig expected = new HiveFormatsConfig()
                .setAvroFileNativeReaderEnabled(true)
                .setAvroFileNativeWriterEnabled(true)
                .setCsvNativeReaderEnabled(true)
                .setCsvNativeWriterEnabled(true)
                .setJsonNativeReaderEnabled(true)
                .setJsonNativeWriterEnabled(true)
                .setOpenXJsonNativeReaderEnabled(true)
                .setOpenXJsonNativeWriterEnabled(true)
                .setRegexNativeReaderEnabled(true)
                .setTextFileNativeReaderEnabled(true)
                .setTextFileNativeWriterEnabled(true)
                .setSequenceFileNativeReaderEnabled(true)
                .setSequenceFileNativeWriterEnabled(true);

        assertFullMapping(properties, expected);
    }
}
