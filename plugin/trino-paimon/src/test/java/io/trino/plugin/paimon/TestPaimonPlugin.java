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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPaimonPlugin
{
    @Test
    void testCreatePaimonConnector()
            throws IOException
    {
        String warehouse = createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        Plugin plugin = new PaimonPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create(
                "paimon",
                ImmutableMap.of("paimon.warehouse", warehouse, "paimon.catalog.type", "filesystem"),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }
}
