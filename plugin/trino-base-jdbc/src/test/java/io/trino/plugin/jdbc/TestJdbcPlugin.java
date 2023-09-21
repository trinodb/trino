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
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.mapping.MappingConfig.CASE_INSENSITIVE_NAME_MATCHING;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;

public class TestJdbcPlugin
{
    @Test
    public void testCreateConnector()
    {
        getConnectorFactory().create("test", TestingH2JdbcModule.createProperties(), new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testRuleBasedIdentifierCanBeUsedTogetherWithCacheBased()
            throws Exception
    {
        getConnectorFactory().create(
                "test",
                ImmutableMap.<String, String>builder()
                        .putAll(TestingH2JdbcModule.createProperties())
                        .put(CASE_INSENSITIVE_NAME_MATCHING, "true")
                        .put(CASE_INSENSITIVE_NAME_MATCHING + ".config-file", createRuleBasedIdentifierMappingFile().toFile().getAbsolutePath())
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        Plugin plugin = new JdbcPlugin("jdbc", new TestingH2JdbcModule());
        return getOnlyElement(plugin.getConnectorFactories());
    }
}
