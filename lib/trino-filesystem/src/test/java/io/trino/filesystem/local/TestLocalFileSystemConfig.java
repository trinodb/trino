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
package io.trino.filesystem.local;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestLocalFileSystemConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LocalFileSystemConfig.class)
                .setLocation(Paths.get(System.getProperty(JAVA_IO_TMPDIR.key()))));
    }

    @Test
    void testExplicitPropertyMappings(@TempDir Path tempDirectory)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("local.location", tempDirectory.toString())
                .buildOrThrow();

        LocalFileSystemConfig expected = new LocalFileSystemConfig()
                .setLocation(tempDirectory);

        assertFullMapping(properties, expected);
    }
}
