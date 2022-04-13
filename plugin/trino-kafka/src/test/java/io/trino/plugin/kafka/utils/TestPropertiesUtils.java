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
package io.trino.plugin.kafka.utils;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static io.trino.plugin.kafka.utils.PropertiesUtils.readProperties;
import static org.testng.Assert.assertEquals;

public class TestPropertiesUtils
{
    @Test
    public void testReadPropertiesOverwritten()
            throws IOException
    {
        Map<String, String> expected = ImmutableMap.<String, String>builder()
                .put("security.protocol", "OVERWRITTEN")
                .put("group.id", "consumer")
                .put("client.id", "producer")
                .buildOrThrow();

        Properties firstProperties = new Properties();
        firstProperties.putAll(ImmutableMap.<String, String>builder()
                .put("security.protocol", "SASL_PLAINTEXT")
                .put("group.id", "consumer")
                .buildOrThrow());
        File firstFile = this.writePropertiesToFile(firstProperties);

        Properties secondProperties = new Properties();
        secondProperties.putAll(ImmutableMap.<String, String>builder()
                .put("security.protocol", "OVERWRITTEN")
                .put("client.id", "producer")
                .buildOrThrow());
        File secondFile = this.writePropertiesToFile(secondProperties);

        Map<String, String> result = readProperties(Arrays.asList(firstFile, secondFile));

        assertEquals(result, expected);
    }

    private File writePropertiesToFile(Properties properties)
            throws IOException
    {
        Path path = Files.createTempFile(null, null);
        try (OutputStream outputStream = new FileOutputStream(path.toString())) {
            properties.store(outputStream, null);
        }
        return path.toFile();
    }
}
