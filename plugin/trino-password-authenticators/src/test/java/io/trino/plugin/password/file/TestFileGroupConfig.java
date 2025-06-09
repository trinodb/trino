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
package io.trino.plugin.password.file;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFileGroupConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(FileGroupConfig.class)
                .setGroupFile(null)
                .setRefreshPeriod(new Duration(5, SECONDS)));
    }

    @Test
    public void testExplicitConfig()
            throws IOException
    {
        Path groupFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("file.group-file", groupFile.toString())
                .put("file.refresh-period", "42s")
                .buildOrThrow();

        FileGroupConfig expected = new FileGroupConfig()
                .setGroupFile(groupFile.toFile())
                .setRefreshPeriod(new Duration(42, SECONDS));

        assertFullMapping(properties, expected);
    }
}
