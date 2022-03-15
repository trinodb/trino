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
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;

public class TestSingleStorePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new SingleStorePlugin();
        ConnectorFactory factory = stream(plugin.getConnectorFactories())
                .filter(connectorFactory -> connectorFactory.getName().equals("singlestore"))
                .collect(toOptional())
                .orElseThrow();
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:singlestore://test"), new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testCreateConnectorLegacyName()
    {
        Plugin plugin = new SingleStorePlugin();
        ConnectorFactory factory = stream(plugin.getConnectorFactories())
                .filter(connectorFactory -> connectorFactory.getName().equals("memsql"))
                .collect(toOptional())
                .orElseThrow();
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:singlestore://test"), new TestingConnectorContext()).shutdown();
    }
}
