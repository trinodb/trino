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
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMerge
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));
    private static final ResolvedFunction NOT = FUNCTIONS.resolveFunction("$not", fromTypes(BOOLEAN));
    private static final Type ROW_TYPE = RowType.anonymousRow(INTEGER, INTEGER, BOOLEAN, TINYINT, INTEGER);

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("schema")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false");

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.installPlugin(
                new MockConnectorPlugin(MockConnectorFactory.builder()
                        .withGetTableHandle((_, schemaTableName) -> {
                            if (schemaTableName.getTableName().equals("test_table_merge_target")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            if (schemaTableName.getTableName().equals("test_table_merge_source")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(_ -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .build()));
        planTester.createCatalog("mock", "mock", ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testMergeWithSimpleSelect()
    {
        // one join
        assertPlan(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED " +
                        "THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED " +
                        "THEN INSERT (column1 ,column2) VALUES (b.column1, b.column2)",
                anyTree(
                        filter(
                                new Case(
                                        ImmutableList.of(new WhenClause(new Call(NOT, ImmutableList.of(new Reference(BOOLEAN, "is_distinct"))), new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) MERGE_TARGET_ROW_MULTIPLE_MATCHES.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("One MERGE target table row matched more than one source row")))), BOOLEAN))),
                                        TRUE),
                                markDistinct("is_distinct", ImmutableList.of("unique_id", "case_number"),
                                        anyTree(
                                                project(ImmutableMap.of(
                                                                "unique_id", expression(new Coalesce(ImmutableList.of(new Reference(BIGINT, "target_unique_id"), new Reference(BIGINT, "source_unique_id")))),
                                                                "field", expression(new Reference(BIGINT, "field")),
                                                                "merge_row", expression(new Reference(ROW_TYPE, "merge_row")),
                                                                "case_number", expression(new FieldReference(new Reference(ROW_TYPE, "merge_row"), 4))),
                                                        project(ImmutableMap.of(
                                                                        "field", expression(new Reference(BIGINT, "field")),
                                                                        "merge_row", expression(new Case(
                                                                                ImmutableList.of(
                                                                                        new WhenClause(new Logical(Logical.Operator.AND, ImmutableList.of(new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "source_present")))))), new Row(ImmutableList.of(new Reference(INTEGER, "column1"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 3L), new Constant(INTEGER, 0L)), ROW_TYPE)),
                                                                                        new WhenClause(new IsNull(new Reference(BOOLEAN, "present")), new Row(ImmutableList.of(new Reference(INTEGER, "column1_0"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 1L), new Constant(INTEGER, 1L)), ROW_TYPE))),
                                                                                new Constant(ROW_TYPE, null))),
                                                                        "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                        "source_unique_id", expression(new Reference(BIGINT, "source_unique_id"))),
                                                                join(RIGHT, builder -> builder
                                                                        .equiCriteria("column1", "column1_0")
                                                                        .left(
                                                                                project(ImmutableMap.of(
                                                                                                "column1", expression(new Reference(INTEGER, "column1")),
                                                                                                "field", expression(new Reference(BIGINT, "field")),
                                                                                                "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                                                "present", expression(new Constant(BOOLEAN, true))),
                                                                                        assignUniqueId("target_unique_id",
                                                                                                tableScan(
                                                                                                        tableHandle -> ((MockConnectorTableHandle) tableHandle).getTableName().getTableName().equals("test_table_merge_target"),
                                                                                                        TupleDomain.all(),
                                                                                                        ImmutableMap.of(
                                                                                                                "column1", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("column1"),
                                                                                                                "field", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("merge_row_id"))))))
                                                                        .right(
                                                                                anyTree(
                                                                                        project(ImmutableMap.of(
                                                                                                        "source_present", expression(new Constant(BOOLEAN, true))),
                                                                                                assignUniqueId(
                                                                                                        "source_unique_id",
                                                                                                        tableScan("test_table_merge_source", ImmutableMap.of("column1_0", "column1", "column2_1", "column2"))))))))))))));
    }

    @Test
    public void testMergeNotMatchedBySourceRejectSourceColumnInPredicate()
    {
        // Source column reference in BY SOURCE AND predicate must be rejected
        assertThatThrownBy(() -> assertPlan(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE AND b.column2 > 0 " +
                        "THEN DELETE",
                anyTree(anyTree())))
                .satisfies(e -> {
                    Throwable cause = e;
                    while (cause.getCause() != null && !(cause instanceof TrinoException)) {
                        cause = cause.getCause();
                    }
                    assertThat(cause)
                            .isInstanceOf(TrinoException.class)
                            .hasMessageContaining("Columns from the source relation cannot be referenced in a WHEN NOT MATCHED BY SOURCE clause");
                    assertThat(((TrinoException) cause).getErrorCode())
                            .isEqualTo(INVALID_COLUMN_REFERENCE.toErrorCode());
                });
    }

    @Test
    public void testMergeNotMatchedBySourceRejectSourceColumnInSetExpression()
    {
        // Source column reference in BY SOURCE UPDATE SET expression must be rejected
        assertThatThrownBy(() -> assertPlan(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE " +
                        "THEN UPDATE SET column2 = b.column2",
                anyTree(anyTree())))
                .satisfies(e -> {
                    Throwable cause = e;
                    while (cause.getCause() != null && !(cause instanceof TrinoException)) {
                        cause = cause.getCause();
                    }
                    assertThat(cause)
                            .isInstanceOf(TrinoException.class)
                            .hasMessageContaining("Columns from the source relation cannot be referenced in a WHEN NOT MATCHED BY SOURCE clause");
                    assertThat(((TrinoException) cause).getErrorCode())
                            .isEqualTo(INVALID_COLUMN_REFERENCE.toErrorCode());
                });
    }

    @Test
    public void testMergeNotMatchedBySourceValidDeleteAccepted()
    {
        // BY SOURCE DELETE with no predicate — valid, should produce a plan without error
        getPlanTester().inTransaction(session -> {
            getPlanTester().createPlan(
                    session,
                    "MERGE INTO test_table_merge_target a " +
                            "USING test_table_merge_source b " +
                            "ON a.column1 = b.column1 " +
                            "WHEN NOT MATCHED BY SOURCE " +
                            "THEN DELETE",
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return null;
        });
    }

    @Test
    public void testMergeNotMatchedBySourceValidDeleteWithTargetPredicateAccepted()
    {
        // BY SOURCE DELETE with target-only predicate — valid
        getPlanTester().inTransaction(session -> {
            getPlanTester().createPlan(
                    session,
                    "MERGE INTO test_table_merge_target a " +
                            "USING test_table_merge_source b " +
                            "ON a.column1 = b.column1 " +
                            "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 " +
                            "THEN DELETE",
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return null;
        });
    }

    @Test
    public void testMergeNotMatchedBySourceValidUpdateWithTargetColumnsOnlyAccepted()
    {
        // BY SOURCE UPDATE with no source column in SET expression — valid
        getPlanTester().inTransaction(session -> {
            getPlanTester().createPlan(
                    session,
                    "MERGE INTO test_table_merge_target a " +
                            "USING test_table_merge_source b " +
                            "ON a.column1 = b.column1 " +
                            "WHEN NOT MATCHED BY SOURCE " +
                            "THEN UPDATE SET column2 = 0",
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return null;
        });
    }

    @Test
    public void testMergeAllThreeClauseKindsAccepted()
    {
        // MATCHED + BY TARGET + BY SOURCE — valid combination requiring FULL OUTER join
        getPlanTester().inTransaction(session -> {
            getPlanTester().createPlan(
                    session,
                    "MERGE INTO test_table_merge_target a " +
                            "USING test_table_merge_source b " +
                            "ON a.column1 = b.column1 " +
                            "WHEN MATCHED THEN UPDATE SET column2 = b.column2 " +
                            "WHEN NOT MATCHED THEN INSERT (column1, column2) VALUES (b.column1, b.column2) " +
                            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return null;
        });
    }

    @Test
    public void testMergeMatchedAndBySourceUsesLeftJoin()
    {
        // MATCHED + BY SOURCE only → LEFT join (target preserved; source may be absent)
        assertThat(findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE")
                .getType())
                .isEqualTo(LEFT);
    }

    @Test
    public void testMergeBySourceOnlyUsesLeftJoin()
    {
        // BY SOURCE only (no MATCHED, no BY TARGET) → LEFT join
        assertThat(findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE")
                .getType())
                .isEqualTo(LEFT);
    }

    @Test
    public void testMergeAllThreeClauseKindsUsesFullJoin()
    {
        // MATCHED + BY TARGET + BY SOURCE → FULL OUTER join
        assertThat(findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED THEN INSERT (column1, column2) VALUES (b.column1, b.column2) " +
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE")
                .getType())
                .isEqualTo(FULL);
    }

    @Test
    public void testMergeMultipleBySourceClausesAccepted()
    {
        // Two BY SOURCE clauses with different predicates — valid; first matching clause wins
        getPlanTester().inTransaction(session -> {
            getPlanTester().createPlan(
                    session,
                    "MERGE INTO test_table_merge_target a " +
                            "USING test_table_merge_source b " +
                            "ON a.column1 = b.column1 " +
                            "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 THEN DELETE " +
                            "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET column2 = 0",
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return null;
        });
    }

    @Test
    public void testBySourcePredicatesPushedDownToTargetScan()
    {
        // All clauses are BY SOURCE with predicates → disjunction pushed as pre-join FilterNode.
        JoinNode join = findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 THEN DELETE");
        assertThat(PlanNodeSearcher.searchFrom(join.getLeft())
                .where(FilterNode.class::isInstance)
                .findFirst())
                .isPresent();
    }

    @Test
    public void testBySourcePredicatesPushedDownMultipleClauses()
    {
        // Multiple BY SOURCE clauses, all with predicates — disjunction of both pushed.
        JoinNode join = findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 > 10 THEN DELETE " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 <= 10 THEN UPDATE SET column2 = 0");
        assertThat(PlanNodeSearcher.searchFrom(join.getLeft())
                .where(FilterNode.class::isInstance)
                .findFirst())
                .isPresent();
    }

    @Test
    public void testBySourcePredicatesNotPushedWhenUnconditionalClause()
    {
        // One BY SOURCE clause has no predicate (catch-all) → no pushdown.
        JoinNode join = findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 THEN DELETE " +
                        "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET column2 = 0");
        assertThat(PlanNodeSearcher.searchFrom(join.getLeft())
                .where(FilterNode.class::isInstance)
                .findFirst())
                .isEmpty();
    }

    @Test
    public void testBySourcePredicatesNotPushedWhenMatchedClausePresent()
    {
        // MATCHED clause present → pre-filtering target rows would suppress MATCHED actions.
        JoinNode join = findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 THEN DELETE");
        assertThat(PlanNodeSearcher.searchFrom(join.getLeft())
                .where(FilterNode.class::isInstance)
                .findFirst())
                .isEmpty();
    }

    @Test
    public void testBySourcePredicatesNotPushedWhenByTargetClausePresent()
    {
        // BY TARGET clause present → pre-filtering target rows would promote source rows
        // to spurious BY TARGET (INSERT) rows.
        JoinNode join = findMergeJoin(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN NOT MATCHED THEN INSERT (column1, column2) VALUES (b.column1, b.column2) " +
                        "WHEN NOT MATCHED BY SOURCE AND a.column2 > 0 THEN DELETE");
        assertThat(PlanNodeSearcher.searchFrom(join.getLeft())
                .where(FilterNode.class::isInstance)
                .findFirst())
                .isEmpty();
    }

    private JoinNode findMergeJoin(String sql)
    {
        return getPlanTester().inTransaction(session -> {
            Plan plan = getPlanTester().createPlan(
                    session,
                    sql,
                    getPlanTester().getPlanOptimizers(true),
                    LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            return (JoinNode) PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(JoinNode.class::isInstance)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No join node found in plan for: " + sql));
        });
    }
}
