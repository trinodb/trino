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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.TopNNode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPushTopNIntoTableScan
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(new MockConnectorTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)));

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String dimensionName = "dimension";
    private static final ColumnHandle dimensionColumn = new TpchColumnHandle(dimensionName, VARCHAR);
    private static final String metricName = "metric";
    private static final ColumnHandle metricColumn = new TpchColumnHandle(metricName, BIGINT);

    private static final ImmutableMap<String, ColumnHandle> assignments = ImmutableMap.of(
            dimensionName, dimensionColumn,
            metricName, metricColumn);

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle)
    {
        return new TableHandle(
                new CatalogName(MOCK_CATALOG),
                tableHandle,
                new ConnectorTransactionHandle() {},
                Optional.empty());
    }

    @Test
    public void testDoesNotFire()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.empty());
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(TEST_TABLE_HANDLE,
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void testPushTopNIntoTableScan()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
            // make the mock connector return a new connectorHandle
            MockConnectorFactory.ApplyTopN applyTopN =
                    (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, true));
            MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(TEST_TABLE_HANDLE,
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            tableScan(
                                    equalTo(connectorHandle),
                                    TupleDomain.all(),
                                    new HashMap<>()));
        }
    }

    @Test
    public void testPushTopNIntoTableScanPartial()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
            // make the mock connector return a new connectorHandle
            MockConnectorFactory.ApplyTopN applyTopN =
                    (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, false));
            MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(TEST_TABLE_HANDLE,
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            topN(1, ImmutableList.of(sort(dimensionName, ASCENDING, FIRST)),
                                    TopNNode.Step.SINGLE,
                                    tableScan(
                                            equalTo(connectorHandle),
                                            TupleDomain.all(),
                                            ImmutableMap.of(
                                                    dimensionName, equalTo(dimensionColumn),
                                                    metricName, equalTo(metricColumn)))));
        }
    }

    private MockConnectorFactory createMockFactory(Map<String, ColumnHandle> assignments, Optional<MockConnectorFactory.ApplyTopN> applyTopN)
    {
        List<ColumnMetadata> metadata = assignments.entrySet().stream()
                .map(entry -> new ColumnMetadata(entry.getKey(), ((TpchColumnHandle) entry.getValue()).getType()))
                .collect(toImmutableList());

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((connectorSession, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_SCHEMA_TABLE) : ImmutableList.of())
                .withGetColumns(schemaTableName -> metadata);

        if (applyTopN.isPresent()) {
            builder = builder.withApplyTopN(applyTopN.get());
        }

        return builder.build();
    }
}
