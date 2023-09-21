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
package io.trino.plugin.thrift.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.drift.server.DriftServer;
import io.trino.Session;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.metadata.TableHandle;
import io.trino.plugin.thrift.ThriftColumnHandle;
import io.trino.plugin.thrift.ThriftPlugin;
import io.trino.plugin.thrift.ThriftTableHandle;
import io.trino.plugin.thrift.ThriftTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.thrift.integration.ThriftQueryRunner.driftServerPort;
import static io.trino.plugin.thrift.integration.ThriftQueryRunner.startThriftServers;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestThriftProjectionPushdown
        extends BaseRuleTest
{
    private static final String TINY_SCHEMA = "tiny";
    private List<DriftServer> servers;

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TINY_SCHEMA)
            .build();

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        try {
            servers = startThriftServers(1, false);
        }
        catch (Throwable t) {
            try {
                cleanup();
            }
            catch (Throwable e) {
                t.addSuppressed(e);
            }

            throw t;
        }

        String addresses = servers.stream()
                .map(server -> "localhost:" + driftServerPort(server))
                .collect(joining(","));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("trino.thrift.client.addresses", addresses)
                .put("trino.thrift.client.connect-timeout", "30s")
                .put("trino-thrift.lookup-requests-concurrency", "2")
                .buildOrThrow();

        LocalQueryRunner runner = LocalQueryRunner.create(SESSION);
        runner.createCatalog(TEST_CATALOG_NAME, getOnlyElement(new ThriftPlugin().getConnectorFactories()), connectorProperties);

        return Optional.of(runner);
    }

    @AfterAll
    public void cleanup()
    {
        if (servers != null) {
            try (Closer closer = Closer.create()) {
                for (DriftServer server : servers) {
                    closer.register(server::shutdown);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            servers = null;
        }
    }

    @Test
    public void testDoesNotFire()
    {
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        String columnName = "orderstatus";
        ColumnHandle columnHandle = new ThriftColumnHandle(columnName, VARCHAR, "", false);

        ConnectorTableHandle tableWithColumns = new ThriftTableHandle(
                TINY_SCHEMA,
                "nation",
                TupleDomain.all(),
                Optional.of(ImmutableSet.of(columnHandle)));

        tester().assertThat(pushProjectionIntoTableScan)
                .on(p -> {
                    Symbol orderStatusSymbol = p.symbol(columnName, VARCHAR);
                    return p.project(
                            Assignments.of(
                                    p.symbol("expr_2", VARCHAR),
                                    orderStatusSymbol.toSymbolReference()),
                            p.tableScan(
                                    new TableHandle(
                                            tester().getCurrentCatalogHandle(),
                                            tableWithColumns,
                                            ThriftTransactionHandle.INSTANCE),
                                    ImmutableList.of(orderStatusSymbol),
                                    ImmutableMap.of(orderStatusSymbol, columnHandle)));
                })
                .doesNotFire();
    }

    @Test
    public void testProjectionPushdown()
    {
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        String columnName = "orderstatus";

        ColumnHandle columnHandle = new ThriftColumnHandle(columnName, VARCHAR, "", false);

        ConnectorTableHandle projectedThriftHandle = new ThriftTableHandle(
                TINY_SCHEMA,
                "nation",
                TupleDomain.all(),
                Optional.of(ImmutableSet.of(columnHandle)));

        tester().assertThat(pushProjectionIntoTableScan)
                .on(p -> {
                    Symbol orderStatusSymbol = p.symbol(columnName, VARCHAR);
                    return p.project(
                            Assignments.of(
                                    p.symbol("expr_2", VARCHAR),
                                    orderStatusSymbol.toSymbolReference()),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA, "nation"),
                                    ImmutableList.of(orderStatusSymbol),
                                    ImmutableMap.of(orderStatusSymbol, columnHandle)));
                })
                .withSession(SESSION)
                .matches(project(
                        ImmutableMap.of("expr_2", expression(new SymbolReference(columnName))),
                        tableScan(
                                projectedThriftHandle::equals,
                                TupleDomain.all(),
                                ImmutableMap.of(columnName, columnHandle::equals))));
    }

    @Test
    public void testPruneColumns()
    {
        PruneTableScanColumns rule = new PruneTableScanColumns(tester().getMetadata());

        ThriftColumnHandle nationKeyColumn = new ThriftColumnHandle("nationKey", VARCHAR, "", false);
        ThriftColumnHandle nameColumn = new ThriftColumnHandle("name", VARCHAR, "", false);

        tester().assertThat(rule)
                .on(p -> {
                    Symbol nationKey = p.symbol(nationKeyColumn.getColumnName(), VARCHAR);
                    Symbol name = p.symbol(nameColumn.getColumnName(), VARCHAR);
                    return p.project(
                            Assignments.of(
                                    p.symbol("expr", VARCHAR),
                                    nationKey.toSymbolReference()),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA, "nation"),
                                    ImmutableList.of(nationKey, name),
                                    ImmutableMap.<Symbol, ColumnHandle>builder()
                                            .put(nationKey, nationKeyColumn)
                                            .put(name, nameColumn)
                                            .buildOrThrow()));
                })
                .withSession(SESSION)
                .matches(project(
                        ImmutableMap.of("expr", expression(new SymbolReference(nationKeyColumn.getColumnName()))),
                        tableScan(
                                new ThriftTableHandle(
                                        TINY_SCHEMA,
                                        "nation",
                                        TupleDomain.all(),
                                        Optional.of(ImmutableSet.of(nationKeyColumn)))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of(nationKeyColumn.getColumnName(), nationKeyColumn::equals))));
    }
}
