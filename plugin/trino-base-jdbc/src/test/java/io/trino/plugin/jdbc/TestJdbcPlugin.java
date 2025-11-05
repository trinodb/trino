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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.Plugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.plugin.base.mapping.MappingConfig.CASE_INSENSITIVE_NAME_MATCHING;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.jdbc.TestingH2JdbcModule.createH2ConnectionUrl;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    @RepeatedTest(100)
    void testConfigurationDoesNotLeakBetweenCatalogs()
    {
        TestingJdbcPlugin plugin = new TestingJdbcPlugin("test_jdbc", TestingJdbcModule::new);
        ConnectorFactory connectorFactory = getOnlyElement(plugin.getConnectorFactories());

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Connector> pushDownEnabledFuture = executor.submit(() -> connectorFactory.create(
                    TestingJdbcModule.CATALOG_WITH_PUSH_DOWN_ENABLED,
                    ImmutableMap.of("connection-url", createH2ConnectionUrl(), "join-pushdown.enabled", "true"),
                    new TestingConnectorContext()));
            Future<Connector> pushDownDisabledFuture = executor.submit(() -> connectorFactory.create(
                    TestingJdbcModule.CATALOG_WITH_PUSH_DOWN_DISABLED,
                    ImmutableMap.of("connection-url", createH2ConnectionUrl(), "join-pushdown.enabled", "false"),
                    new TestingConnectorContext()));

            AtomicReference<Connector> catalogWithPushDownEnabled = new AtomicReference<>();
            AtomicReference<Connector> catalogWithPushDownDisabled = new AtomicReference<>();
            assertThatCode(() -> {
                catalogWithPushDownEnabled.set(pushDownEnabledFuture.get());
                catalogWithPushDownDisabled.set(pushDownDisabledFuture.get());
            }).doesNotThrowAnyException();

            catalogWithPushDownEnabled.get().shutdown();
            catalogWithPushDownDisabled.get().shutdown();
        }
    }

    private static class TestingJdbcPlugin
            extends JdbcPlugin
    {
        public TestingJdbcPlugin(String name, Supplier<Module> module)
        {
            super(name, module);
        }
    }

    private static class TestingJdbcModule
            extends AbstractConfigurationAwareModule
    {
        public static final String CATALOG_WITH_PUSH_DOWN_ENABLED = "catalogWithPushDownEnabled";
        public static final String CATALOG_WITH_PUSH_DOWN_DISABLED = "catalogWithPushDownDisabled";

        @Override
        protected void setup(Binder binder)
        {
            install(conditionalModule(
                    JdbcMetadataConfig.class,
                    JdbcMetadataConfig::isJoinPushdownEnabled,
                    new ModuleCheckingThatPushDownCanBeEnabled()));
            install(new TestingH2JdbcModule());
        }
    }

    private static class ModuleCheckingThatPushDownCanBeEnabled
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(PushDownCanBeEnabledChecker.class).in(Scopes.SINGLETON);
        }
    }

    private static class PushDownCanBeEnabledChecker
    {
        @Inject
        public PushDownCanBeEnabledChecker(CatalogName catalogName)
        {
            if (!TestingJdbcModule.CATALOG_WITH_PUSH_DOWN_ENABLED.equals(catalogName.toString())) {
                throw new RuntimeException("Catalog '%s' should not have push-down enabled".formatted(catalogName));
            }
        }
    }

    private static ConnectorFactory getConnectorFactory()
    {
        Plugin plugin = new JdbcPlugin("jdbc", TestingH2JdbcModule::new);
        return getOnlyElement(plugin.getConnectorFactories());
    }
}
