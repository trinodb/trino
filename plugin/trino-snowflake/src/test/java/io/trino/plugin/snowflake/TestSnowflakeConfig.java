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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSnowflakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeConfig.class)
                .setAccount(null)
                .setDatabase(null)
                .setRole(null)
                .setWarehouse(null)
                .setHttpProxy(null)
                .setPrivateKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.account", "MYACCOUNT")
                .put("snowflake.database", "MYDATABASE")
                .put("snowflake.role", "MYROLE")
                .put("snowflake.warehouse", "MYWAREHOUSE")
                .put("snowflake.http-proxy", "MYPROXY")
                .put("snowflake.private-key", "MYPRIVATEKEY")
                .buildOrThrow();

        SnowflakeConfig expected = new SnowflakeConfig()
                .setAccount("MYACCOUNT")
                .setDatabase("MYDATABASE")
                .setRole("MYROLE")
                .setWarehouse("MYWAREHOUSE")
                .setHttpProxy("MYPROXY")
                .setPrivateKey("MYPRIVATEKEY");

        assertFullMapping(properties, expected);
    }
}
