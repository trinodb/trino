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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
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
import static java.util.concurrent.TimeUnit.HOURS;

public class TestPhoenixConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PhoenixConfig.class)
                .setConnectionUrl(null)
                .setResourceConfigFiles(ImmutableList.of())
                .setMaxScansPerSplit(20)
                .setReuseConnection(true)
                .setServerScanPageTimeout(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path configFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("phoenix.connection-url", "jdbc:phoenix:localhost:2181:/hbase")
                .put("phoenix.config.resources", configFile.toString())
                .put("phoenix.max-scans-per-split", "1")
                .put("query.reuse-connection", "false")
                .put("phoenix.server-scan-page-timeout", "11h")
                .buildOrThrow();

        PhoenixConfig expected = new PhoenixConfig()
                .setConnectionUrl("jdbc:phoenix:localhost:2181:/hbase")
                .setResourceConfigFiles(ImmutableList.of(configFile.toString()))
                .setMaxScansPerSplit(1)
                .setServerScanPageTimeout(new Duration(11, HOURS))
                .setReuseConnection(false);

        assertFullMapping(properties, expected);
    }
}
