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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

final class TestFakerPlugin
{
    @Test
    void testCreateConnector()
    {
        FakerPlugin plugin = new FakerPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.of("bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }
}
