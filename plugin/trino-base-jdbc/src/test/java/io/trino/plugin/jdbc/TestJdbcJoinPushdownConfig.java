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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.trino.plugin.jdbc.JoinPushdownStrategy.AUTOMATIC;
import static io.trino.plugin.jdbc.JoinPushdownStrategy.EAGER;

public class TestJdbcJoinPushdownConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(JdbcJoinPushdownConfig.class)
                .setJoinPushdownStrategy(AUTOMATIC)
                .setJoinPushdownAutomaticMaxTableSize(null)
                .setJoinPushdownAutomaticMaxJoinToTablesRatio(1.25));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("join-pushdown.strategy", "EAGER")
                .put("experimental.join-pushdown.automatic.max-table-size", "10MB")
                .put("experimental.join-pushdown.automatic.max-join-to-tables-ratio", "2.0")
                .buildOrThrow();

        JdbcJoinPushdownConfig expected = new JdbcJoinPushdownConfig()
                .setJoinPushdownStrategy(EAGER)
                .setJoinPushdownAutomaticMaxTableSize(DataSize.valueOf("10MB"))
                .setJoinPushdownAutomaticMaxJoinToTablesRatio(2.0);

        assertFullMapping(properties, expected);
    }
}
