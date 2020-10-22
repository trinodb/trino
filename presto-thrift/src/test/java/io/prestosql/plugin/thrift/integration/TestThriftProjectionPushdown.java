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
package io.prestosql.plugin.thrift.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.drift.server.DriftServer;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.thrift.ThriftColumnHandle;
import io.prestosql.plugin.thrift.ThriftPlugin;
import io.prestosql.plugin.thrift.ThriftTableHandle;
import io.prestosql.plugin.thrift.ThriftTransactionHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.rule.PruneTableScanColumns;
import io.prestosql.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.thrift.integration.ThriftQueryRunner.driftServerPort;
import static io.prestosql.plugin.thrift.integration.ThriftQueryRunner.startThriftServers;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

public class TestThriftProjectionPushdown
        extends BaseRuleTest
{
    private static final String CATALOG = "test";
    private static final String TINY_SCHEMA = "tiny";
    private List<DriftServer> servers;

    private static final ThriftTableHandle NATION_THRIFT_TABLE = new ThriftTableHandle(new SchemaTableName(TINY_SCHEMA, "nation"));

    private static final TableHandle NATION_TABLE = new TableHandle(
            new CatalogName(CATALOG),
            NATION_THRIFT_TABLE,
            ThriftTransactionHandle.INSTANCE,
            Optional.empty());

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
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
                .put("presto.thrift.client.addresses", addresses)
                .put("presto.thrift.client.connect-timeout", "30s")
                .put("presto-thrift.lookup-requests-concurrency", "2")
                .build();

        LocalQueryRunner runner = LocalQueryRunner.create(SESSION);
        runner.createCatalog(CATALOG, getOnlyElement(new ThriftPlugin().getConnectorFactories()), connectorProperties);

        return Optional.of(runner);
    }

    @AfterClass
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
        PushProjectionIntoTableScan optimizer = new PushProjectionIntoTableScan(
                tester().getMetadata(),
                new TypeAnalyzer(new SqlParser(), tester().getMetadata()));

        String columnName = "orderstatus";
        ColumnHandle columnHandle = new ThriftColumnHandle(columnName, VARCHAR, "", false);

        ConnectorTableHandle tableWithColumns = new ThriftTableHandle(
                TINY_SCHEMA,
                "nation",
                TupleDomain.all(),
                Optional.of(ImmutableSet.of(columnHandle)));

        tester().assertThat(optimizer)
                .on(p -> {
                    Symbol orderStatusSymbol = p.symbol(columnName, VARCHAR);
                    return p.project(
                            Assignments.of(
                                    p.symbol("expr_2", VARCHAR),
                                    orderStatusSymbol.toSymbolReference()),
                            p.tableScan(
                                    new TableHandle(
                                            new CatalogName(CATALOG),
                                            tableWithColumns,
                                            ThriftTransactionHandle.INSTANCE,
                                            Optional.empty()),
                                    ImmutableList.of(orderStatusSymbol),
                                    ImmutableMap.of(orderStatusSymbol, columnHandle)));
                })
                .doesNotFire();
    }

    @Test
    public void testProjectionPushdown()
    {
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getMetadata(),
                new TypeAnalyzer(new SqlParser(), tester().getMetadata()));

        TableHandle inputTableHandle = NATION_TABLE;
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
                                    inputTableHandle,
                                    ImmutableList.of(orderStatusSymbol),
                                    ImmutableMap.of(orderStatusSymbol, columnHandle)));
                })
                .withSession(SESSION)
                .matches(project(
                        ImmutableMap.of("expr_2", expression(new SymbolReference(columnName))),
                        tableScan(
                                equalTo(projectedThriftHandle),
                                TupleDomain.all(),
                                ImmutableMap.of(columnName, equalTo(columnHandle)))));
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
                                    NATION_TABLE,
                                    ImmutableList.of(nationKey, name),
                                    ImmutableMap.<Symbol, ColumnHandle>builder()
                                            .put(nationKey, nationKeyColumn)
                                            .put(name, nameColumn)
                                            .build()));
                })
                .withSession(SESSION)
                .matches(project(
                        ImmutableMap.of("expr", expression(new SymbolReference(nationKeyColumn.getColumnName()))),
                        tableScan(
                                equalTo(new ThriftTableHandle(
                                        TINY_SCHEMA,
                                        "nation",
                                        TupleDomain.all(),
                                        Optional.of(ImmutableSet.of(nationKeyColumn)))),
                                TupleDomain.all(),
                                ImmutableMap.of(nationKeyColumn.getColumnName(), equalTo(nationKeyColumn)))));
    }
}
