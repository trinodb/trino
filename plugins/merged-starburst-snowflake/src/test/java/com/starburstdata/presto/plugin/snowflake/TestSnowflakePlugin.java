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
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static org.testng.Assert.assertEquals;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new SnowflakePlugin();
        List<ConnectorFactory> connectorFactories = stream(plugin.getConnectorFactories())
                .collect(toImmutableList());
        assertEquals(connectorFactories.size(), 2);

        connectorFactories.get(0).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "test",
                        "snowflake.role", "test",
                        "snowflake.database", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
        connectorFactories.get(1).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "test",
                        "snowflake.impersonation-type", "ROLE",
                        "snowflake.database", "test",
                        "snowflake.stage-schema", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
    }
}
