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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.MockConnectorFactory.ApplyFilter;
import static io.trino.connector.MockConnectorFactory.ApplyProjection;
import static io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableScanRedirectionWithPushdown
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName sourceTable = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String sourceColumnNameA = "source_col_a";
    private static final ColumnHandle sourceColumnHandleA = new MockConnectorColumnHandle(sourceColumnNameA, INTEGER);
    private static final String sourceColumnNameB = "source_col_b";
    private static final ColumnHandle sourceColumnHandleB = new MockConnectorColumnHandle(sourceColumnNameB, INTEGER);
    private static final String sourceColumnNameC = "source_col_c";
    private static final ColumnHandle sourceColumnHandleC = new MockConnectorColumnHandle(sourceColumnNameC, VARCHAR);

    private static final SchemaTableName destinationTable = new SchemaTableName("target_schema", "target_table");
    private static final String destinationColumnNameA = "destination_col_a";
    private static final ColumnHandle destinationColumnHandleA = new MockConnectorColumnHandle(destinationColumnNameA, INTEGER);
    private static final String destinationColumnNameB = "destination_col_b";
    private static final ColumnHandle destinationColumnHandleB = new MockConnectorColumnHandle(destinationColumnNameB, INTEGER);

    private static final Map<ColumnHandle, String> redirectionMappingA = ImmutableMap.of(sourceColumnHandleA, destinationColumnNameA);

    private static final Map<ColumnHandle, String> redirectionMappingAB = ImmutableMap.of(
            sourceColumnHandleA, destinationColumnNameA,
            sourceColumnHandleB, destinationColumnNameB);

    private static final Map<ColumnHandle, String> typeMismatchedRedirectionMappingBC = ImmutableMap.of(
            sourceColumnHandleB, destinationColumnNameB,
            sourceColumnHandleC, destinationColumnNameA);

    @Test
    public void testRedirectionAfterProjectionPushdown()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect that source_col_a is projected
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                this::mockApplyRedirectAfterProjectionPushdown,
                Optional.of(this::mockApplyProjection),
                Optional.empty())) {
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table",
                    output(
                            ImmutableList.of("DEST_COL"),
                            tableScan("target_table", ImmutableMap.of("DEST_COL", destinationColumnNameA))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a, source_col_b FROM test_table",
                    output(
                            ImmutableList.of("SOURCE_COLA", "SOURCE_COLB"),
                            tableScan(TEST_TABLE, ImmutableMap.of("SOURCE_COLA", sourceColumnNameA, "SOURCE_COLB", sourceColumnNameB))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table WHERE source_col_a > 0",
                    output(
                            ImmutableList.of("DEST_COL"),
                            filter(
                                    "DEST_COL > 0",
                                    tableScan(
                                            equalTo(new MockConnectorTableHandle(destinationTable)),
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", equalTo(destinationColumnHandleA))))));
        }
    }

    @Test
    public void testRedirectionAfterPredicatePushdownIntoTableScan()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect a filter
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(redirectionMappingA, Optional.empty()),
                Optional.empty(),
                Optional.of(getMockApplyFilter(ImmutableSet.of(sourceColumnHandleA, destinationColumnHandleA))))) {
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table WHERE source_col_a = 1",
                    output(
                            ImmutableList.of("DEST_COL"),
                            tableScan(
                                    equalTo(new MockConnectorTableHandle(destinationTable)),
                                    TupleDomain.withColumnDomains(ImmutableMap.of(equalTo(destinationColumnHandleA), singleValue(INTEGER, 1L))),
                                    ImmutableMap.of("DEST_COL", equalTo(destinationColumnHandleA)))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table",
                    output(
                            ImmutableList.of("SOURCE_COL"),
                            tableScan(TEST_TABLE, ImmutableMap.of("SOURCE_COL", sourceColumnNameA))));
        }
    }

    @Test
    public void testPredicatePushdownAfterRedirect()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(redirectionMappingAB, Optional.empty()),
                Optional.empty(),
                Optional.of(getMockApplyFilter(ImmutableSet.of(sourceColumnHandleA, destinationColumnHandleB))))) {
            // Only 'source_col_a = 1' will get pushed down into source table scan
            // Only 'dest_col_b = 2' will get pushed down into destination table scan
            // This test verifies that the Filter('dest_col_a = 1') produced by redirection
            // does not prevent pushdown of 'dest_col_b = 2' into destination table scan
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a, source_col_b FROM test_table WHERE source_col_a = 1 AND source_col_b = 2",
                    output(
                            ImmutableList.of("DEST_COL_A", "DEST_COL_B"),
                            filter(
                                    "DEST_COL_A = 1",
                                    tableScan(
                                            equalTo(new MockConnectorTableHandle(destinationTable)),
                                            TupleDomain.withColumnDomains(ImmutableMap.of(equalTo(destinationColumnHandleB), singleValue(INTEGER, 2L))),
                                            ImmutableMap.of(
                                                    "DEST_COL_A", equalTo(destinationColumnHandleA),
                                                    "DEST_COL_B", equalTo(destinationColumnHandleB))))));
        }
    }

    @Test
    public void testRedirectAfterColumnPruningOnPushedDownPredicate()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(redirectionMappingAB, Optional.of(ImmutableSet.of(sourceColumnHandleB))),
                Optional.of(this::mockApplyProjection),
                Optional.of(getMockApplyFilter(ImmutableSet.of(sourceColumnHandleA, destinationColumnHandleA))))) {
            // After 'source_col_a = 1' is pushed into source table scan, it's possible for 'source_col_a' table scan assignment to be pruned
            // Redirection results in Project('dest_col_b') -> Filter('dest_col_a = 1') -> TableScan for such case
            // Subsequent PPD and column pruning rules simplify the above as supported by the destination connector
            assertPlan(
                    queryRunner,
                    "SELECT source_col_b FROM test_table WHERE source_col_a = 1",
                    output(
                            ImmutableList.of("DEST_COL_B"),
                            tableScan(
                                    equalTo(new MockConnectorTableHandle(destinationTable)),
                                    // PushProjectionIntoTableScan does not preserve enforced constraint
                                    // (issue: https://github.com/trinodb/trino/issues/6029)
                                    TupleDomain.all(),
                                    ImmutableMap.of("DEST_COL_B", equalTo(destinationColumnHandleB)))));
        }
    }

    @Test
    public void testPredicateTypeMismatch()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(typeMismatchedRedirectionMappingBC, Optional.of(ImmutableSet.of(sourceColumnHandleB))),
                Optional.of(this::mockApplyProjection),
                Optional.of(getMockApplyFilter(ImmutableSet.of(sourceColumnHandleC))))) {
            // After 'source_col_c = 1' is pushed into source table scan, it's possible for 'source_col_c' table scan assignment to be pruned
            // Redirection results in Project('dest_col_b') -> Filter('dest_col_c = 1') -> TableScan for such case
            // but dest_col_a has mismatched type compared to source domain
            transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .execute(MOCK_SESSION, session -> {
                        assertThatThrownBy(() -> queryRunner.createPlan(session, "SELECT source_col_b FROM test_table WHERE source_col_c = 'foo'", WarningCollector.NOOP))
                                .isInstanceOf(TrinoException.class)
                                .hasMessageMatching("Redirected column mock_catalog.target_schema.target_table.destination_col_a has type integer, different from source column .*MockConnectorTableHandle.*source_col_c.* type: varchar");
                    });
        }
    }

    private LocalQueryRunner createLocalQueryRunner(
            ApplyTableScanRedirect applyTableScanRedirect,
            Optional<ApplyProjection> applyProjection,
            Optional<ApplyFilter> applyFilter)
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(MOCK_SESSION);
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withGetTableHandle((session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(name -> {
                    if (name.equals(sourceTable)) {
                        return ImmutableList.of(
                                new ColumnMetadata(sourceColumnNameA, INTEGER),
                                new ColumnMetadata(sourceColumnNameB, INTEGER),
                                new ColumnMetadata(sourceColumnNameC, VARCHAR));
                    }
                    else if (name.equals(destinationTable)) {
                        return ImmutableList.of(
                                new ColumnMetadata(destinationColumnNameA, INTEGER),
                                new ColumnMetadata(destinationColumnNameB, INTEGER));
                    }
                    throw new IllegalArgumentException();
                })
                .withApplyTableScanRedirect(applyTableScanRedirect);
        applyProjection.ifPresent(builder::withApplyProjection);
        applyFilter.ifPresent(builder::withApplyFilter);

        queryRunner.createCatalog(MOCK_CATALOG, builder.build(), ImmutableMap.of());
        return queryRunner;
    }

    private Optional<ProjectionApplicationResult<ConnectorTableHandle>> mockApplyProjection(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;

        List<Variable> variables = projections.stream()
                .map(Variable.class::cast)
                .collect(toImmutableList());
        List<ColumnHandle> newColumns = variables.stream()
                .map(variable -> assignments.get(variable.getName()))
                .collect(toImmutableList());
        if (handle.getColumns().isPresent() && newColumns.equals(handle.getColumns().get())) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectionApplicationResult<>(
                        new MockConnectorTableHandle(handle.getTableName(), handle.getConstraint(), Optional.of(newColumns)),
                        projections,
                        variables.stream()
                                .map(variable -> new Assignment(
                                        variable.getName(),
                                        assignments.get(variable.getName()),
                                        ((MockConnectorColumnHandle) assignments.get(variable.getName())).getType()))
                                .collect(toImmutableList())));
    }

    private Optional<TableScanRedirectApplicationResult> mockApplyRedirectAfterProjectionPushdown(
            ConnectorSession session,
            ConnectorTableHandle handle)
    {
        MockConnectorTableHandle mockConnectorTable = (MockConnectorTableHandle) handle;
        Optional<List<ColumnHandle>> projectedColumns = mockConnectorTable.getColumns();
        if (projectedColumns.isEmpty()) {
            return Optional.empty();
        }
        List<String> projectedColumnNames = projectedColumns.get().stream()
                .map(MockConnectorColumnHandle.class::cast)
                .map(MockConnectorColumnHandle::getName)
                .collect(toImmutableList());
        if (!projectedColumnNames.equals(ImmutableList.of(sourceColumnNameA))) {
            return Optional.empty();
        }
        return Optional.of(
                new TableScanRedirectApplicationResult(
                        new CatalogSchemaTableName(MOCK_CATALOG, destinationTable),
                        redirectionMappingA,
                        mockConnectorTable.getConstraint()
                                .transform(MockConnectorColumnHandle.class::cast)
                                .transform(MockConnectorColumnHandle::getName)));
    }

    private ApplyFilter getMockApplyFilter(Set<ColumnHandle> pushdownColumns)
    {
        // returns a mock implementation of applyFilter which allows predicate pushdown only for pushdownColumns
        return (session, table, constraint) -> {
            MockConnectorTableHandle handle = (MockConnectorTableHandle) table;

            TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
            TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary()
                    .filter((columnHandle, domain) -> pushdownColumns.contains(columnHandle)));
            if (oldDomain.equals(newDomain)) {
                return Optional.empty();
            }

            return Optional.of(
                    new ConstraintApplicationResult<>(
                            new MockConnectorTableHandle(handle.getTableName(), newDomain, Optional.empty()),
                            constraint.getSummary()
                                    .filter((columnHandle, domain) -> !pushdownColumns.contains(columnHandle))));
        };
    }

    private ApplyTableScanRedirect getMockApplyRedirectAfterPredicatePushdown(
            Map<ColumnHandle, String> redirectionMapping,
            Optional<Set<ColumnHandle>> requiredProjections)
    {
        return (session, handle) -> {
            MockConnectorTableHandle mockConnectorTable = (MockConnectorTableHandle) handle;
            // make sure we do redirection after predicate is pushed down
            if (mockConnectorTable.getConstraint().isAll()) {
                return Optional.empty();
            }
            Optional<List<ColumnHandle>> projectedColumns = mockConnectorTable.getColumns();
            if (requiredProjections.isPresent()
                    && (projectedColumns.isEmpty() || !requiredProjections.get().equals(ImmutableSet.copyOf(projectedColumns.get())))) {
                return Optional.empty();
            }
            return Optional.of(
                    new TableScanRedirectApplicationResult(
                            new CatalogSchemaTableName(MOCK_CATALOG, destinationTable),
                            redirectionMapping,
                            mockConnectorTable.getConstraint()
                                    .transform(MockConnectorColumnHandle.class::cast)
                                    .transform(redirectionMapping::get)));
        };
    }

    void assertPlan(LocalQueryRunner queryRunner, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }
}
