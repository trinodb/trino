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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDatabendPlugin
{
    private ConnectorFactory getConnectorFactory()
    {
        Plugin plugin = new DatabendPlugin();
        return getOnlyElement(plugin.getConnectorFactories());
    }

    private Map<String, String> getBaseConfig(String connectionUrl)
    {
        return ImmutableMap.of(
                "connection-url", connectionUrl,
                "bootstrap.quiet", "true");
    }

    @Test
    void testCreateConnectorWithValidUrl()
    {
        // Arrange
        ConnectorFactory factory = getConnectorFactory();
        Map<String, String> config = getBaseConfig("jdbc:databend://test");

        // Act & Assert
        factory.create(
                        "test",
                        config,
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testCreateConnectorWithInvalidUrl()
    {
        // Arrange
        ConnectorFactory factory = getConnectorFactory();
        Map<String, String> config = getBaseConfig("test");

        // Act & Assert
        assertThatThrownBy(() ->
                factory.create(
                        "test",
                        config,
                        new TestingConnectorContext()))
                .hasMessageContaining("Invalid JDBC URL for Databend connector");
    }
}
