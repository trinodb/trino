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
import io.trino.spi.expression.Call;
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

import static io.trino.connector.MockConnectorFactory.ApplyFilter;
import static io.trino.connector.MockConnectorFactory.ApplyProjection;
import static io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tests.BogusType.BOGUS;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableScanRedirectionWithPushdown
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName SOURCE_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String SOURCE_COLUMN_NAME_A = "source_col_a";
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_A = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_A, INTEGER);
    private static final String SOURCE_COLUMN_NAME_B = "source_col_b";
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_B = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_B, INTEGER);
    private static final String SOURCE_COLUMN_NAME_C = "source_col_c";
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_C = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_C, VARCHAR);
    private static final String SOURCE_COLUMN_NAME_D = "source_col_d";
    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_D = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_D, ROW_TYPE);

    private static final SchemaTableName DESTINATION_TABLE = new SchemaTableName("target_schema", "target_table");
    private static final String DESTINATION_COLUMN_NAME_A = "destination_col_a";
    private static final ColumnHandle DESTINATION_COLUMN_HANDLE_A = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_A, INTEGER);
    private static final String DESTINATION_COLUMN_NAME_B = "destination_col_b";
    private static final ColumnHandle DESTINATION_COLUMN_HANDLE_B = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_B, INTEGER);
    private static final String DESTINATION_COLUMN_NAME_C = "destination_col_c";
    private static final String DESTINATION_COLUMN_NAME_D = "destination_col_d";

    private static final Map<ColumnHandle, String> REDIRECTION_MAPPING_A = ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A);

    private static final Map<ColumnHandle, String> REDIRECTION_MAPPING_AB = ImmutableMap.of(
            SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A,
            SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B);

    private static final Map<ColumnHandle, String> TYPE_MISMATCHED_REDIRECTION_MAPPING_BC = ImmutableMap.of(
            SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B,
            SOURCE_COLUMN_HANDLE_C, DESTINATION_COLUMN_NAME_A);

    private static final Map<ColumnHandle, String> ROW_TYPE_REDIRECTION_MAPPING_AD = ImmutableMap.of(
            SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A,
            SOURCE_COLUMN_HANDLE_D, DESTINATION_COLUMN_NAME_C);

    private static final Map<ColumnHandle, String> BOGUS_REDIRECTION_MAPPING_BC = ImmutableMap.of(
            SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B,
            SOURCE_COLUMN_HANDLE_C, DESTINATION_COLUMN_NAME_D);

    @Test
    public void testRedirectionAfterProjectionPushdown()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect that source_col_a is projected
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                mockApplyRedirectAfterProjectionPushdown(REDIRECTION_MAPPING_A, Optional.of(ImmutableSet.of(SOURCE_COLUMN_HANDLE_A))),
                Optional.of(this::mockApplyProjection),
                Optional.empty())) {
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table",
                    output(
                            ImmutableList.of("DEST_COL"),
                            tableScan("target_table", ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_NAME_A))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a, source_col_b FROM test_table",
                    output(
                            ImmutableList.of("SOURCE_COLA", "SOURCE_COLB"),
                            tableScan(TEST_TABLE, ImmutableMap.of("SOURCE_COLA", SOURCE_COLUMN_NAME_A, "SOURCE_COLB", SOURCE_COLUMN_NAME_B))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table WHERE source_col_a > 0",
                    output(
                            ImmutableList.of("DEST_COL"),
                            filter(
                                    "DEST_COL > 0",
                                    tableScan(
                                            new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals)))));
        }
    }

    @Test
    public void testRedirectionAfterPredicatePushdownIntoTableScan()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect a filter
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(REDIRECTION_MAPPING_A, Optional.empty()),
                Optional.empty(),
                Optional.of(getMockApplyFilter(ImmutableSet.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_HANDLE_A))))) {
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table WHERE source_col_a = 1",
                    output(
                            ImmutableList.of("DEST_COL"),
                            tableScan(
                                    new MockConnectorTableHandle(
                                            DESTINATION_TABLE,
                                            TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_HANDLE_A, singleValue(INTEGER, 1L))),
                                            Optional.empty())::equals,
                                    TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_HANDLE_A::equals, singleValue(INTEGER, 1L))),
                                    ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals))));

            assertPlan(
                    queryRunner,
                    "SELECT source_col_a FROM test_table",
                    output(
                            ImmutableList.of("SOURCE_COL"),
                            tableScan(TEST_TABLE, ImmutableMap.of("SOURCE_COL", SOURCE_COLUMN_NAME_A))));
        }
    }

    @Test
    public void testPredicatePushdownAfterRedirect()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(REDIRECTION_MAPPING_AB, Optional.empty()),
                Optional.empty(),
                Optional.of(getMockApplyFilter(ImmutableSet.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_HANDLE_B))))) {
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
                                            new MockConnectorTableHandle(
                                                    DESTINATION_TABLE,
                                                    TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_HANDLE_B, singleValue(INTEGER, 2L))),
                                                    Optional.empty())::equals,
                                            TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_HANDLE_B::equals, singleValue(INTEGER, 2L))),
                                            ImmutableMap.of(
                                                    "DEST_COL_A", DESTINATION_COLUMN_HANDLE_A::equals,
                                                    "DEST_COL_B", DESTINATION_COLUMN_HANDLE_B::equals)))));
        }
    }

    @Test
    public void testRedirectAfterColumnPruningOnPushedDownPredicate()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(REDIRECTION_MAPPING_AB, Optional.of(ImmutableSet.of(SOURCE_COLUMN_HANDLE_B))),
                Optional.of(this::mockApplyProjection),
                Optional.of(getMockApplyFilter(ImmutableSet.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_HANDLE_A))))) {
            // After 'source_col_a = 1' is pushed into source table scan, it's possible for 'source_col_a' table scan assignment to be pruned
            // Redirection results in Project('dest_col_b') -> Filter('dest_col_a = 1') -> TableScan for such case
            // Subsequent PPD and column pruning rules simplify the above as supported by the destination connector
            assertPlan(
                    queryRunner,
                    "SELECT source_col_b FROM test_table WHERE source_col_a = 1",
                    output(
                            ImmutableList.of("DEST_COL_B"),
                            tableScan(
                                    new MockConnectorTableHandle(
                                            DESTINATION_TABLE,
                                            TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_HANDLE_A, singleValue(INTEGER, 1L))),
                                            Optional.of(ImmutableList.of(DESTINATION_COLUMN_HANDLE_B)))::equals,
                                    // PushProjectionIntoTableScan does not preserve enforced constraint
                                    // (issue: https://github.com/trinodb/trino/issues/6029)
                                    TupleDomain.all(),
                                    ImmutableMap.of("DEST_COL_B", DESTINATION_COLUMN_HANDLE_B::equals))));
        }
    }

    @Test
    public void testPredicateTypeWithCoercion()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(TYPE_MISMATCHED_REDIRECTION_MAPPING_BC, Optional.of(ImmutableSet.of(SOURCE_COLUMN_HANDLE_B))),
                Optional.of(this::mockApplyProjection),
                Optional.of(getMockApplyFilter(ImmutableSet.of(SOURCE_COLUMN_HANDLE_C))))) {
            // After 'source_col_c = 1' is pushed into source table scan, it's possible for 'source_col_c' table scan assignment to be pruned
            // Redirection results in Project('dest_col_b') -> Filter('dest_col_c = 1') -> TableScan for such case
            // but dest_col_a has mismatched type compared to source domain
            assertPlan(
                    queryRunner,
                    "SELECT source_col_b FROM test_table WHERE source_col_c = 'foo'",
                    output(
                            ImmutableList.of("DEST_COL_B"),
                            project(ImmutableMap.of("DEST_COL_B", expression("DEST_COL_B")),
                                    filter("CAST(DEST_COL_A AS VARCHAR) = VARCHAR 'foo'",
                                            tableScan(
                                                    new MockConnectorTableHandle(
                                                            DESTINATION_TABLE,
                                                            TupleDomain.all(),
                                                            Optional.of(ImmutableList.of(DESTINATION_COLUMN_HANDLE_B, DESTINATION_COLUMN_HANDLE_A)))::equals,
                                                    // PushProjectionIntoTableScan does not preserve enforced constraint
                                                    // (issue: https://github.com/trinodb/trino/issues/6029)
                                                    TupleDomain.all(),
                                                    ImmutableMap.of(
                                                            "DEST_COL_B", DESTINATION_COLUMN_HANDLE_B::equals,
                                                            "DEST_COL_A", DESTINATION_COLUMN_HANDLE_A::equals))))));
        }
    }

    @Test
    public void testPredicateTypeMismatchWithMissingCoercion()
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                getMockApplyRedirectAfterPredicatePushdown(BOGUS_REDIRECTION_MAPPING_BC, Optional.of(ImmutableSet.of(SOURCE_COLUMN_HANDLE_B))),
                Optional.of(this::mockApplyProjection),
                Optional.of(getMockApplyFilter(ImmutableSet.of(SOURCE_COLUMN_HANDLE_C))))) {
            // After 'source_col_d = 1' is pushed into source table scan, it's possible for 'source_col_c' table scan assignment to be pruned
            // Redirection results in Project('dest_col_b') -> Filter('dest_col_d = 1') -> TableScan for such case
            // but dest_col_d has mismatched type compared to source domain
            transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .execute(MOCK_SESSION, session -> {
                        assertThatThrownBy(() -> queryRunner.createPlan(session, "SELECT source_col_b FROM test_table WHERE source_col_c = 'foo'", WarningCollector.NOOP, createPlanOptimizersStatsCollector()))
                                .isInstanceOf(TrinoException.class)
                                .hasMessageMatching("Cast not possible from redirected column mock_catalog.target_schema.target_table.destination_col_d with type Bogus to source column .*mock_catalog.test_schema.test_table.*source_col_c.* with type: varchar");
                    });
        }
    }

    @Test
    public void testRedirectionBeforeDeferencePushdown()
    {
        // make the mock connector return a table scan on destination table only if
        // the connector can detect that source_col_a and source_col_d is projected
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(
                mockApplyRedirectAfterProjectionPushdown(ROW_TYPE_REDIRECTION_MAPPING_AD, Optional.of(ImmutableSet.of(SOURCE_COLUMN_HANDLE_A, SOURCE_COLUMN_HANDLE_D))),
                Optional.of(this::mockApplyProjection),
                Optional.empty())) {
            // Pushdown of dereference for source_col_d.a into table scan results in a new column handle
            // Table scan redirection would not take place if dereference pushdown has already taken place before redirection
            ColumnHandle destinationColumnHandleC0 = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_C + "#0", BIGINT);
            assertPlan(
                    queryRunner,
                    "SELECT source_col_a, source_col_d.a FROM test_table",
                    output(
                            ImmutableList.of("DEST_COL_A", "DEST_COL_C#0"),
                            tableScan(
                                    new MockConnectorTableHandle(
                                            DESTINATION_TABLE,
                                            TupleDomain.all(),
                                            Optional.of(ImmutableList.of(DESTINATION_COLUMN_HANDLE_A, destinationColumnHandleC0)))::equals,
                                    TupleDomain.all(),
                                    ImmutableMap.of(
                                            "DEST_COL_A", DESTINATION_COLUMN_HANDLE_A::equals,
                                            "DEST_COL_C#0", destinationColumnHandleC0::equals))));
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
                    if (name.equals(SOURCE_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(SOURCE_COLUMN_NAME_A, INTEGER),
                                new ColumnMetadata(SOURCE_COLUMN_NAME_B, INTEGER),
                                new ColumnMetadata(SOURCE_COLUMN_NAME_C, VARCHAR),
                                new ColumnMetadata(SOURCE_COLUMN_NAME_D, ROW_TYPE));
                    }
                    if (name.equals(DESTINATION_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_A, INTEGER),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_B, INTEGER),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_C, ROW_TYPE),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_D, BOGUS));
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
            ConnectorExpression newVariable;
            ColumnHandle newColumnHandle;
            Type type = projection.getType();
            if (projection instanceof Variable variable) {
                newVariableName = variable.getName();
                newVariable = new Variable(newVariableName, type);
                newColumnHandle = assignments.get(newVariableName);
            }
            else if (projection instanceof FieldDereference dereference) {
                if (!(dereference.getTarget() instanceof Variable variable)) {
                    throw new UnsupportedOperationException();
                }
                String dereferenceTargetName = variable.getName();
                newVariableName = ((MockConnectorColumnHandle) assignments.get(dereferenceTargetName)).getName() + "#" + dereference.getField();
                newVariable = new Variable(newVariableName, type);
                newColumnHandle = new MockConnectorColumnHandle(newVariableName, type);
            }
            else if (projection instanceof Call call) {
                if (!(CAST_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1)) {
                    throw new UnsupportedOperationException();
                }
                // Avoid CAST pushdown into the connector
                newVariableName = ((Variable) call.getArguments().get(0)).getName();
                newVariable = projection;
                newColumnHandle = assignments.get(newVariableName);
                type = call.getArguments().get(0).getType();
            }
            else {
                throw new UnsupportedOperationException();
            }

            newColumnsBuilder.add(newColumnHandle);
            outputExpressions.add(newVariable);
            outputAssignments.add(new Assignment(newVariableName, newColumnHandle, type));
        }

        List<ColumnHandle> newColumns = newColumnsBuilder.build();
        if (handle.getColumns().isPresent() && newColumns.equals(handle.getColumns().get())) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectionApplicationResult<>(
                        new MockConnectorTableHandle(handle.getTableName(), handle.getConstraint(), Optional.of(newColumns)),
                        outputExpressions.build(),
                        outputAssignments.build(),
                        false));
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
                                    .filter((columnHandle, domain) -> !pushdownColumns.contains(columnHandle)),
                            false));
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
                            new CatalogSchemaTableName(MOCK_CATALOG, DESTINATION_TABLE),
                            redirectionMapping,
                            mockConnectorTable.getConstraint()
                                    .transformKeys(MockConnectorColumnHandle.class::cast)
                                    .filter((columnHandle, domain) -> redirectionMapping.containsKey(columnHandle))
                                    .transformKeys(redirectionMapping::get)));
        };
    }

    void assertPlan(LocalQueryRunner queryRunner, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }
}
