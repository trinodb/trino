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
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
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
import static io.trino.connector.MockConnectorFactory.ApplyFilter;
import static io.trino.connector.MockConnectorFactory.ApplyProjection;
import static io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Arrays.asList;
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
    private static final String sourceColumnNameD = "source_col_d";
    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));
    private static final ColumnHandle sourceColumnHandleD = new MockConnectorColumnHandle(sourceColumnNameD, ROW_TYPE);

    private static final SchemaTableName destinationTable = new SchemaTableName("target_schema", "target_table");
    private static final String destinationColumnNameA = "destination_col_a";
    private static final ColumnHandle destinationColumnHandleA = new MockConnectorColumnHandle(destinationColumnNameA, INTEGER);
    private static final String destinationColumnNameB = "destination_col_b";
    private static final ColumnHandle destinationColumnHandleB = new MockConnectorColumnHandle(destinationColumnNameB, INTEGER);
    private static final String destinationColumnNameC = "destination_col_c";

    private static final Map<ColumnHandle, String> redirectionMappingA = ImmutableMap.of(sourceColumnHandleA, destinationColumnNameA);

    private static final Map<ColumnHandle, String> redirectionMappingAB = ImmutableMap.of(
            sourceColumnHandleA, destinationColumnNameA,
            sourceColumnHandleB, destinationColumnNameB);

    private static final Map<ColumnHandle, String> typeMismatchedRedirectionMappingBC = ImmutableMap.of(
            sourceColumnHandleB, destinationColumnNameB,
            sourceColumnHandleC, destinationColumnNameA);

    private static final Map<ColumnHandle, String> rowTypeRedirectionMappingAD = ImmutableMap.of(
            sourceColumnHandleA, destinationColumnNameA,
            sourceColumnHandleD, destinationColumnNameC);

    @Test
    public void testRedirectionAfterProjectionPushdown()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect that source_col_a is projected
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                mockApplyRedirectAfterProjectionPushdown(redirectionMappingA, Optional.of(ImmutableSet.of(sourceColumnHandleA))),
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

    @Test
    public void testRedirectionBeforeDeferencePushdown()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect that source_col_a and source_col_d is projected
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                mockApplyRedirectAfterProjectionPushdown(rowTypeRedirectionMappingAD, Optional.of(ImmutableSet.of(sourceColumnHandleA, sourceColumnHandleD))),
                Optional.of(this::mockApplyProjection),
                Optional.empty())) {
            // Pushdown of dereference for source_col_d.a into table scan results in a new column handle
            // Table scan redirection would not take place if dereference pushdown has already taken place before redirection
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a, source_col_d.a FROM test_table",
                    output(
                            ImmutableList.of("DEST_COL_A", "DEST_COL_C#0"),
                            tableScan(
                                    equalTo(new MockConnectorTableHandle(destinationTable)),
                                    TupleDomain.all(),
                                    ImmutableMap.of(
                                            "DEST_COL_A", equalTo(destinationColumnHandleA),
                                            "DEST_COL_C#0", equalTo(new MockConnectorColumnHandle(sourceColumnNameD + "#0", ROW_TYPE))))));
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
                                new ColumnMetadata(sourceColumnNameC, VARCHAR),
                                new ColumnMetadata(sourceColumnNameD, ROW_TYPE));
                    }
                    else if (name.equals(destinationTable)) {
                        return ImmutableList.of(
                                new ColumnMetadata(destinationColumnNameA, INTEGER),
                                new ColumnMetadata(destinationColumnNameB, INTEGER),
                                new ColumnMetadata(destinationColumnNameC, ROW_TYPE));
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

        ImmutableList.Builder<ColumnHandle> newColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> outputExpressions = ImmutableList.builder();
        ImmutableList.Builder<Assignment> outputAssignments = ImmutableList.builder();

        for (ConnectorExpression projection : projections) {
            String newVariableName;
            ColumnHandle newColumnHandle;
            if (projection instanceof Variable) {
                newVariableName = ((Variable) projection).getName();
                newColumnHandle = assignments.get(newVariableName);
            }
            else if (projection instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) projection;
                if (!(dereference.getTarget() instanceof Variable)) {
                    throw new UnsupportedOperationException();
                }
                newVariableName = ((Variable) dereference.getTarget()).getName() + "#" + dereference.getField();
                newColumnHandle = new MockConnectorColumnHandle(newVariableName, projection.getType());
            }
            else {
                throw new UnsupportedOperationException();
            }

            Variable newVariable = new Variable(newVariableName, projection.getType());
            newColumnsBuilder.add(newColumnHandle);
            outputExpressions.add(newVariable);
            outputAssignments.add(new Assignment(newVariableName, newColumnHandle, projection.getType()));
        }

        List<ColumnHandle> newColumns = newColumnsBuilder.build();
        if (handle.getColumns().isPresent() && newColumns.equals(handle.getColumns().get())) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectionApplicationResult<>(
                        new MockConnectorTableHandle(handle.getTableName(), handle.getConstraint(), Optional.of(newColumns)),
                        outputExpressions.build(),
                        outputAssignments.build()));
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

    private ApplyTableScanRedirect mockApplyRedirectAfterProjectionPushdown(
            Map<ColumnHandle, String> redirectionMapping,
            Optional<Set<ColumnHandle>> requiredProjections)
    {
        return getMockApplyRedirect(redirectionMapping, requiredProjections, false);
    }

    private ApplyTableScanRedirect getMockApplyRedirectAfterPredicatePushdown(
            Map<ColumnHandle, String> redirectionMapping,
            Optional<Set<ColumnHandle>> requiredProjections)
    {
        return getMockApplyRedirect(redirectionMapping, requiredProjections, true);
    }

    private ApplyTableScanRedirect getMockApplyRedirect(
            Map<ColumnHandle, String> redirectionMapping,
            Optional<Set<ColumnHandle>> requiredProjections,
            boolean requirePredicatePushdown)
    {
        return (session, handle) -> {
            MockConnectorTableHandle mockConnectorTable = (MockConnectorTableHandle) handle;
            // make sure we do redirection after predicate is pushed down
            if (requirePredicatePushdown && mockConnectorTable.getConstraint().isAll()) {
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
