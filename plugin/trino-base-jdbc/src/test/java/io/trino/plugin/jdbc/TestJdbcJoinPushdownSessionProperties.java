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
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.SessionPropertiesUtil.assertDefaultProperties;
import static io.trino.testing.SessionPropertiesUtil.assertExplicitProperties;

public class TestJdbcJoinPushdownSessionProperties
{
    @Test
    public void testDefaults()
    {
        JdbcJoinPushdownSessionProperties provider = new JdbcJoinPushdownSessionProperties(new JdbcJoinPushdownConfig());

        Map<String, Object> properties = new HashMap<>();
        properties.put("join_pushdown_strategy", JoinPushdownStrategy.AUTOMATIC);
        properties.put("join_pushdown_automatic_max_table_size", null);
        properties.put("join_pushdown_automatic_max_join_to_tables_ratio", 1.25);

        assertDefaultProperties(provider.getSessionProperties(), properties);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        JdbcJoinPushdownSessionProperties provider = new JdbcJoinPushdownSessionProperties(new JdbcJoinPushdownConfig());

        Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put("join_pushdown_strategy", JoinPushdownStrategy.EAGER)
                .put("join_pushdown_automatic_max_table_size", true)
                .put("join_pushdown_automatic_max_join_to_tables_ratio", 1.0)
                .buildOrThrow();

        assertExplicitProperties(provider.getSessionProperties(), properties);
    }
}
