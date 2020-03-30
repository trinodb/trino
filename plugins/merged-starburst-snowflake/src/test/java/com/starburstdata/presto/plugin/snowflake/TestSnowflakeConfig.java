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
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeImpersonationType.NONE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeImpersonationType.ROLE;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSnowflakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeConfig.class)
                .setImpersonationType(NONE)
                .setWarehouse(null)
                .setDatabase(null)
                .setRole(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("snowflake.impersonation-type", "ROLE")
                .put("snowflake.warehouse", "warehouse")
                .put("snowflake.database", "database")
                .put("snowflake.role", "role")
                .build();

        SnowflakeConfig expected = new SnowflakeConfig()
                .setImpersonationType(ROLE)
                .setWarehouse("warehouse")
                .setDatabase("database")
                .setRole("role");

        assertFullMapping(properties, expected);
    }
}
