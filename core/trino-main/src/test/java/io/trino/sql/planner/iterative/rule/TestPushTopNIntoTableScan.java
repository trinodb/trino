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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.TopNNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPushTopNIntoTableScan
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(TEST_SCHEMA).build();

    private static final String dimensionName = "dimension";
    private static final ColumnHandle dimensionColumn = new TpchColumnHandle(dimensionName, VARCHAR);
    private static final String metricName = "metric";
    private static final ColumnHandle metricColumn = new TpchColumnHandle(metricName, BIGINT);

    private static final Map<String, ColumnHandle> assignments = ImmutableMap.of(
            dimensionName, dimensionColumn,
            metricName, metricColumn);

    @Test
    public void testDoesNotFire()
    {
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.empty());
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushSingleTopNIntoTableScan()
    {
        MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
        // make the mock connector return a new connectorHandle
        MockConnectorFactory.ApplyTopN applyTopN =
                (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, true, false));
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .matches(
                            tableScan(
                                    connectorHandle::equals,
                                    TupleDomain.all(),
                                    new HashMap<>()));
        }
    }

    @Test
    public void testPushSingleTopNIntoTableScanNotGuaranteed()
    {
        MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
        // make the mock connector return a new connectorHandle
        MockConnectorFactory.ApplyTopN applyTopN =
                (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, false, false));
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension),
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .matches(
                            topN(1, ImmutableList.of(sort(dimensionName, ASCENDING, FIRST)),
                                    TopNNode.Step.SINGLE,
                                    tableScan(
                                            connectorHandle::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of(
                                                    dimensionName, dimensionColumn::equals,
                                                    metricName, metricColumn::equals))));
        }
    }

    @Test
    public void testPushPartialTopNIntoTableScan()
    {
        MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
        // make the mock connector return a new connectorHandle
        MockConnectorFactory.ApplyTopN applyTopN =
                (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, true, false));
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension), TopNNode.Step.PARTIAL,
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .matches(
                            tableScan(
                                    connectorHandle::equals,
                                    TupleDomain.all(),
                                    new HashMap<>()));
        }
    }

    @Test
    public void testPushPartialTopNIntoTableScanNotGuaranteed()
    {
        MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
        // make the mock connector return a new connectorHandle
        MockConnectorFactory.ApplyTopN applyTopN =
                (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, false, false));
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension), TopNNode.Step.PARTIAL,
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .matches(
                            topN(1, ImmutableList.of(sort(dimensionName, ASCENDING, FIRST)),
                                    TopNNode.Step.PARTIAL,
                                    tableScan(
                                            connectorHandle::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of(
                                                    dimensionName, dimensionColumn::equals,
                                                    metricName, metricColumn::equals))));
        }
    }

    /**
     * Ensure FINAL TopN can be pushed into table scan.
     * <p>
     * In case of TopN over outer join, TopN may become eligible for push down
     * only after PARTIAL TopN was pushed down and only then the join was
     * pushed down as well -- the connector may decide to accept Join pushdown
     * only after it learns there is TopN in play which limits results size.
     * <p>
     * Thus the optimization sequence can be:
     * <ol>
     * <li>Try to push Join into Table Scan -- connector rejects that (e.g. too big data set size)
     * <li>Create FINAL/PARTIAL TopN
     * <li>Push PARTIAL TopN through Outer Join
     * <li>Push PARTIAL TopN into Table Scan -- connector accepts that.
     * <li>Push Join into Table Scan -- connector now accepts join pushdown.
     * <li>Push FINAL TopN into Table Scan
     * </ol>
     */
    @Test
    public void testPushFinalTopNIntoTableScan()
    {
        MockConnectorTableHandle connectorHandle = new MockConnectorTableHandle(TEST_SCHEMA_TABLE);
        // make the mock connector return a new connectorHandle
        MockConnectorFactory.ApplyTopN applyTopN =
                (session, handle, topNCount, sortItems, tableAssignments) -> Optional.of(new TopNApplicationResult<>(connectorHandle, true, false));
        MockConnectorFactory mockFactory = createMockFactory(assignments, Optional.of(applyTopN));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new PushTopNIntoTableScan(ruleTester.getMetadata()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol dimension = p.symbol(dimensionName, VARCHAR);
                        Symbol metric = p.symbol(metricName, BIGINT);
                        return p.topN(1, ImmutableList.of(dimension), TopNNode.Step.FINAL,
                                p.tableScan(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(dimension, metric),
                                        ImmutableMap.of(
                                                dimension, dimensionColumn,
                                                metric, metricColumn)));
                    })
                    .matches(
                            tableScan(
                                    connectorHandle::equals,
                                    TupleDomain.all(),
                                    new HashMap<>()));
        }
    }

    private MockConnectorFactory createMockFactory(Map<String, ColumnHandle> assignments, Optional<MockConnectorFactory.ApplyTopN> applyTopN)
    {
        List<ColumnMetadata> metadata = assignments.entrySet().stream()
                .map(entry -> new ColumnMetadata(entry.getKey(), ((TpchColumnHandle) entry.getValue()).type()))
                .collect(toImmutableList());

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((connectorSession, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_TABLE) : ImmutableList.of())
                .withGetColumns(schemaTableName -> metadata);

        if (applyTopN.isPresent()) {
            builder = builder.withApplyTopN(applyTopN.get());
        }

        return builder.build();
    }
}
