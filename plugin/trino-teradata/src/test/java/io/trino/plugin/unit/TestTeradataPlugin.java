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

package io.trino.plugin.unit;

import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.plugin.teradata.TeradataPlugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTeradataPlugin
{
    @Test
    public void testCreateConnector()
    {
        TeradataPlugin plugin = new TeradataPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(JdbcConnectorFactory.class);

        factory.create(
                        "test",
                        Map.of(
                                "connection-url", "jdbc:teradata://test/"),
                        new TestingConnectorContext())
                .shutdown();
    }
}
