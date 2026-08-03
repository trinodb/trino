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
package io.trino.plugin.doris;

import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

final class TestDorisPlugin
{
    @Test
    void testCreateConnector()
    {
        DorisPlugin plugin = new DorisPlugin();

        Iterator<ConnectorFactory> factories = plugin.getConnectorFactories().iterator();
        ConnectorFactory factory = factories.next();
        factory.create(
                        "test",
                        Map.of(
                                "bootstrap.quiet", "true",
                                "doris.jdbc-url", "jdbc:mysql://127.0.0.1:9030"),
                        new TestingConnectorContext())
                .shutdown();
    }
}
