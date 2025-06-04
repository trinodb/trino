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
import io.airlift.drift.server.DriftServer;
import io.trino.Session;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.thrift.ThriftColumnHandle;
import io.trino.plugin.thrift.ThriftPlugin;
import io.trino.plugin.thrift.ThriftTableHandle;
import io.trino.plugin.thrift.ThriftTransactionHandle;
import io.trino.plugin.thrift.integration.ThriftQueryRunner.StartedServers;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.thrift.integration.ThriftQueryRunner.driftServerPort;
import static io.trino.plugin.thrift.integration.ThriftQueryRunner.startThriftServers;
import static io.trino.spi.type.BigintType.BIGINT;
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
    private StartedServers startedServers;

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TINY_SCHEMA)
            .build();

    @Override
    protected Optional<PlanTester> createPlanTester()
    {
        try {
            startedServers = startThriftServers(1, false);
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

        String addresses = startedServers.servers().stream()
                .map(server -> "localhost:" + driftServerPort(server))
                .collect(joining(","));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("trino.thrift.client.addresses", addresses)
                .put("trino.thrift.client.connect-timeout", "30s")
                .put("trino-thrift.lookup-requests-concurrency", "2")
                .buildOrThrow();

        PlanTester runner = PlanTester.create(SESSION);
        runner.createCatalog(TEST_CATALOG_NAME, getOnlyElement(new ThriftPlugin().getConnectorFactories()), connectorProperties);

        return Optional.of(runner);
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (startedServers != null) {
            try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
                for (DriftServer server : startedServers.servers()) {
                    closer.register(server::shutdown);
                }
                startedServers.resources().forEach(closer::register);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            startedServers = null;
        }
    }

    @Test
    public void testDoesNotFire()
    {
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                new ScalarStatsCalculator(tester().getPlannerContext()));

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
                new ScalarStatsCalculator(tester().getPlannerContext()));

        String columnName = "orderstatus";

        ColumnHandle columnHandle = new ThriftColumnHandle(columnName, VARCHAR, "", false);

        ConnectorTableHandle projectedThriftHandle = new ThriftTableHandle(
                TINY_SCHEMA,
                "nation",
                TupleDomain.all(),
                Optional.of(ImmutableSet.of(columnHandle)));

        tester().assertThat(pushProjectionIntoTableScan)
                .withSession(SESSION)
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
                .matches(project(
                        ImmutableMap.of("expr_2", expression(new Reference(VARCHAR, columnName))),
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
                .withSession(SESSION)
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
                .matches(project(
                        ImmutableMap.of("expr", expression(new Reference(BIGINT, nationKeyColumn.getColumnName()))),
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
