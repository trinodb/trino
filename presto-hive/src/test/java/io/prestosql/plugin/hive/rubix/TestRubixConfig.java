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
package io.prestosql.plugin.hive.rubix;

import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.spi.CacheConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRubixConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RubixConfig.class)
                .setBookKeeperServerPort(CacheConfig.DEFAULT_BOOKKEEPER_SERVER_PORT)
                .setDataTransferServerPort(CacheConfig.DEFAULT_DATA_TRANSFER_SERVER_PORT)
                .setCacheLocation(null)
                .setReadMode(RubixConfig.ReadMode.ASYNC)
                .setStartServerOnCoordinator(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.cache.read-mode", "read-through")
                .put("hive.cache.location", "/some-directory")
                .put("hive.cache.bookkeeper-port", "1234")
                .put("hive.cache.data-transfer-port", "1235")
                .put("hive.cache.start-server-on-coordinator", "true")
                .build();

        RubixConfig expected = new RubixConfig()
                .setReadMode(RubixConfig.ReadMode.READ_THROUGH)
                .setCacheLocation("/some-directory")
                .setBookKeeperServerPort(1234)
                .setDataTransferServerPort(1235)
                .setStartServerOnCoordinator(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testReadModeFromString()
    {
        assertThat(RubixConfig.ReadMode.fromString("async")).isEqualTo(RubixConfig.ReadMode.ASYNC);
        assertThat(RubixConfig.ReadMode.fromString("read-through")).isEqualTo(RubixConfig.ReadMode.READ_THROUGH);
    }
}
