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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AccessControl;
import io.trino.spi.Plugin;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionManager;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class RuleTester
        implements Closeable
{
    private final Metadata metadata;
    private final Session session;
    private final LocalQueryRunner queryRunner;
    private final TransactionManager transactionManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final AccessControl accessControl;
    private final TypeAnalyzer typeAnalyzer;
    private final FunctionManager functionManager;

    public static RuleTester defaultRuleTester()
    {
        return builder().build();
    }

    public RuleTester(LocalQueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.session = queryRunner.getDefaultSession();
        this.metadata = queryRunner.getMetadata();
        this.functionManager = queryRunner.getFunctionManager();
        this.transactionManager = queryRunner.getTransactionManager();
        this.splitManager = queryRunner.getSplitManager();
        this.pageSourceManager = queryRunner.getPageSourceManager();
        this.accessControl = queryRunner.getAccessControl();
        this.typeAnalyzer = createTestingTypeAnalyzer(queryRunner.getPlannerContext());
    }

    public RuleAssert assertThat(Rule<?> rule)
    {
        return new RuleAssert(metadata, functionManager, queryRunner.getStatsCalculator(), queryRunner.getEstimatedExchangesCostCalculator(), session, rule, transactionManager, accessControl);
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }

    public PlannerContext getPlannerContext()
    {
        return queryRunner.getPlannerContext();
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public FunctionManager getFunctionManager()
    {
        return functionManager;
    }

    public Session getSession()
    {
        return session;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public TypeAnalyzer getTypeAnalyzer()
    {
        return typeAnalyzer;
    }

    public CatalogHandle getCurrentCatalogHandle()
    {
        return queryRunner.getCatalogHandle(session.getCatalog().orElseThrow());
    }

    public TableHandle getCurrentCatalogTableHandle(String schemaName, String tableName)
    {
        return queryRunner.getTableHandle(session.getCatalog().orElseThrow(), schemaName, tableName);
    }

    public LocalQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final List<Plugin> plugins = new ArrayList<>();
        private final Map<String, String> sessionProperties = new HashMap<>();
        private Optional<Integer> nodeCountForStats = Optional.empty();
        private ConnectorFactory defaultCatalogConnectorFactory = new TpchConnectorFactory(1);

        private Builder() {}

        public Builder addPlugin(Plugin plugin)
        {
            plugins.add(requireNonNull(plugin, "plugin is null"));
            return this;
        }

        public Builder addPlugins(List<Plugin> plugins)
        {
            this.plugins.addAll(requireNonNull(plugins, "plugins is null"));
            return this;
        }

        public Builder addSessionProperty(String key, String value)
        {
            sessionProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
            return this;
        }

        public Builder withNodeCountForStats(int count)
        {
            this.nodeCountForStats = Optional.of(count);
            return this;
        }

        public Builder withDefaultCatalogConnectorFactory(ConnectorFactory defaultCatalogConnectorFactory)
        {
            this.defaultCatalogConnectorFactory = defaultCatalogConnectorFactory;
            return this;
        }

        public RuleTester build()
        {
            Session.SessionBuilder sessionBuilder = testSessionBuilder()
                    .setCatalog(TEST_CATALOG_NAME)
                    .setSchema("tiny")
                    .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

            for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
                sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
            }

            Session session = sessionBuilder.build();

            LocalQueryRunner queryRunner = nodeCountForStats
                    .map(nodeCount -> LocalQueryRunner.builder(session)
                            .withNodeCountForStats(nodeCount)
                            .build())
                    .orElseGet(() -> LocalQueryRunner.create(session));

            queryRunner.createCatalog(
                    session.getCatalog().orElseThrow(),
                    defaultCatalogConnectorFactory,
                    ImmutableMap.of());
            plugins.forEach(queryRunner::installPlugin);

            return new RuleTester(queryRunner);
        }
    }
}
