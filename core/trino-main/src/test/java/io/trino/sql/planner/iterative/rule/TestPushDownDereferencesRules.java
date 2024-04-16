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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.JsonType.JSON;
import static java.util.Collections.singletonList;

public class TestPushDownDereferencesRules
        extends BaseRuleTest
{
    private static final RowType ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testDoesNotFire()
    {
        // rule does not fire for symbols
        tester().assertThat(new PushDownDereferenceThroughFilter())
                .on(p ->
                        p.filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 5L)),
                                p.values(p.symbol("x"))))
                .doesNotFire();

        // Pushdown is not enabled if dereferences come from an expression that is not a simple dereference chain
        tester().assertThat(new PushDownDereferenceThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1", rowType(field("x", BIGINT), field("y", BIGINT))), new FieldReference(new Cast(new Row(ImmutableList.of(new Reference(ROW_TYPE, "a"), new Reference(BIGINT, "b"))), rowType(field("f1", rowType(field("x", BIGINT), field("y", BIGINT))), field("f2", BIGINT))), 0),
                                        p.symbol("expr_2"), new FieldReference(new FieldReference(new Cast(new Row(ImmutableList.of(new Reference(ROW_TYPE, "a"), new Reference(BIGINT, "b"))), rowType(field("f1", rowType(field("x", BIGINT), field("y", BIGINT))), field("f2", BIGINT))), 0), 1)),
                                p.project(
                                        Assignments.of(
                                                p.symbol("a", ROW_TYPE), new Reference(ROW_TYPE, "a"),
                                                p.symbol("b"), new Reference(BIGINT, "b")),
                                        p.values(p.symbol("a", ROW_TYPE), p.symbol("b")))))
                .doesNotFire();

        // Does not fire when base symbols are referenced along with the dereferences
        tester().assertThat(new PushDownDereferenceThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", ROW_TYPE), new Reference(ROW_TYPE, "a"),
                                        p.symbol("a_x"), new FieldReference(new Reference(ROW_TYPE, "a"), 0)),
                                p.project(
                                        Assignments.of(p.symbol("a", ROW_TYPE), new Reference(ROW_TYPE, "a")),
                                        p.values(p.symbol("a", ROW_TYPE)))))
                .doesNotFire();
    }

    @Test
    public void testPushdownDereferenceThroughProject()
    {
        tester().assertThat(new PushDownDereferenceThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), new FieldReference(new Reference(ROW_TYPE, "msg"), 0)),
                                p.project(
                                        Assignments.of(
                                                p.symbol("y"), new Reference(BIGINT, "y"),
                                                p.symbol("msg", ROW_TYPE), new Reference(ROW_TYPE, "msg")),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("y")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression(new Reference(BIGINT, "msg_x"))),
                                strictProject(
                                        ImmutableMap.of(
                                                "msg_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg"), 0)),
                                                "y", expression(new Reference(BIGINT, "y")),
                                                "msg", expression(new Reference(BIGINT, "msg"))),
                                        values("msg", "y"))));
    }

    @Test
    public void testPushDownDereferenceThroughJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughJoin())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("right_y"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 1))
                                        .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("unreferenced_symbol")),
                                        p.values(p.symbol("msg2", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", expression(new Reference(BIGINT, "x")))
                                        .put("right_y", expression(new Reference(BIGINT, "y")))
                                        .put("z", expression(new Reference(BIGINT, "z")))
                                        .buildOrThrow(),
                                join(INNER, builder -> builder
                                        .left(
                                                strictProject(
                                                        ImmutableMap.of(
                                                                "x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)),
                                                                "msg1", expression(new Reference(ROW_TYPE, "msg1")),
                                                                "unreferenced_symbol", expression(new Reference(BIGINT, "unreferenced_symbol"))),
                                                        values("msg1", "unreferenced_symbol")))
                                        .right(
                                                strictProject(
                                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                                .put("y", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 1)))
                                                                .put("z", expression(new Reference(BIGINT, "z")))
                                                                .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                                .buildOrThrow(),
                                                        values("msg2", "z"))))));

        // Verify pushdown for filters
        tester().assertThat(new PushDownDereferenceThroughJoin())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", ROW_TYPE.getFields().get(0).getType()), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0),
                                        p.symbol("expr_2", ROW_TYPE), new Reference(ROW_TYPE, "msg2")),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", ROW_TYPE)),
                                        p.values(p.symbol("msg2", ROW_TYPE)),
                                        new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0), new FieldReference(new Reference(ROW_TYPE, "msg2"), 1))), new Constant(BIGINT, 10L)))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "expr", expression(new Reference(BIGINT, "msg1_x")),
                                        "expr_2", expression(new Reference(ROW_TYPE, "msg2"))),
                                join(INNER, builder -> builder
                                        .filter(new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 1))), new Constant(BIGINT, 10L)))
                                        .left(
                                                strictProject(
                                                        ImmutableMap.of(
                                                                "msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)),
                                                                "msg1", expression(new Reference(ROW_TYPE, "msg1"))),
                                                        values("msg1")))
                                        .right(values("msg2")))));
    }

    @Test
    public void testPushdownDereferencesThroughSemiJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughSemiJoin())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .build(),
                                p.semiJoin(
                                        p.symbol("msg2", ROW_TYPE),
                                        p.symbol("filtering_msg", ROW_TYPE),
                                        p.symbol("match"),
                                        Optional.empty(),
                                        Optional.empty(),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)),
                                        p.values(p.symbol("filtering_msg", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new Reference(BIGINT, "expr")))
                                        .put("msg2_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0)))   // Not pushed down because msg2 is sourceJoinSymbol
                                        .buildOrThrow(),
                                semiJoin(
                                        "msg2",
                                        "filtering_msg",
                                        "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)),
                                                        "msg1", expression(new Reference(ROW_TYPE, "msg1")),
                                                        "msg2", expression(new Reference(ROW_TYPE, "msg2"))),
                                                values("msg1", "msg2")),
                                        values("filtering_msg"))));
    }

    @Test
    public void testPushdownDereferencesThroughUnnest()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        tester().assertThat(new PushDownDereferenceThroughUnnest())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), new FieldReference(new Reference(ROW_TYPE, "msg"), 0)),
                                p.unnest(
                                        ImmutableList.of(p.symbol("msg", ROW_TYPE)),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("arr", arrayType), ImmutableList.of(p.symbol("field")))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("arr", arrayType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression(new Reference(BIGINT, "msg_x"))),
                                unnest(
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg"), 0)),
                                                        "msg", expression(new Reference(ROW_TYPE, "msg")),
                                                        "arr", expression(new Reference(arrayType, "arr"))),
                                                values("msg", "arr")))));

        // Test with dereferences on unnested column
        RowType rowType = rowType(field("f1", BIGINT), field("f2", BIGINT));
        ArrayType nestedColumnType = new ArrayType(rowType(field("f1", BIGINT), field("f2", rowType)));
        ResolvedFunction subscript = FUNCTIONS.resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(nestedColumnType, BIGINT));

        tester().assertThat(new PushDownDereferenceThroughUnnest())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("deref_replicate", rowType.getFields().get(1).getType()), new FieldReference(new Reference(rowType, "replicate"), 1),
                                        p.symbol("deref_unnest", nestedColumnType.getElementType()), new Call(subscript, ImmutableList.of(new Reference(nestedColumnType, "unnested_row"), new Constant(BIGINT, 2L)))),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", rowType)),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(
                                                        p.symbol("nested", nestedColumnType),
                                                        ImmutableList.of(p.symbol("unnested_bigint", BIGINT), p.symbol("unnested_row", rowType)))),
                                        p.values(p.symbol("replicate", rowType), p.symbol("nested", nestedColumnType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "deref_replicate", expression(new Reference(BIGINT, "symbol")),
                                        "deref_unnest", expression(new Call(subscript, ImmutableList.of(new Reference(nestedColumnType, "unnested_row"), new Constant(BIGINT, 2L))))),    // not pushed down
                                unnest(
                                        ImmutableList.of("replicate", "symbol"),
                                        ImmutableList.of(unnestMapping("nested", ImmutableList.of("unnested_bigint", "unnested_row"))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "symbol", expression(new FieldReference(new Reference(rowType, "replicate"), 1)),
                                                        "replicate", expression(new Reference(rowType, "replicate")),
                                                        "nested", expression(new Reference(nestedColumnType, "nested"))),
                                                values("replicate", "nested")))));
    }

    @Test
    public void testExtractDereferencesFromFilterAboveScan()
    {
        TableHandle testTable = new TableHandle(
                TEST_CATALOG_HANDLE,
                new TpchTableHandle("sf1", "orders", 1.0),
                TestingTransactionHandle.create());

        RowType nestedRowType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("nested"), ROW_TYPE)));
        tester().assertThat(new ExtractDereferencesFromFilterAboveScan())
                .on(p ->
                        p.filter(
                                new Logical(AND, ImmutableList.of(
                                        new Comparison(NOT_EQUAL, new FieldReference(new FieldReference(new Reference(nestedRowType, "a"), 0), 0), new Constant(BIGINT, 5L)),
                                        new Comparison(EQUAL, new FieldReference(new Reference(ROW_TYPE, "b"), 1), new Constant(BIGINT, 2L)),
                                        new Not(new IsNull(new Cast(new FieldReference(new Reference(nestedRowType, "a"), 0), JSON))))),
                                p.tableScan(
                                        testTable,
                                        ImmutableList.of(p.symbol("a", nestedRowType), p.symbol("b", ROW_TYPE)),
                                        ImmutableMap.of(
                                                p.symbol("a", nestedRowType), new TpchColumnHandle("a", nestedRowType),
                                                p.symbol("b", ROW_TYPE), new TpchColumnHandle("b", ROW_TYPE)))))
                .matches(project(
                        filter(
                                new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(BIGINT, "expr"), new Constant(BIGINT, 5L)), new Comparison(EQUAL, new Reference(BIGINT, "expr_0"), new Constant(BIGINT, 2L)), new Not(new IsNull(new Cast(new Reference(ROW_TYPE, "expr_1"), JSON))))),
                                strictProject(
                                        ImmutableMap.of(
                                                "expr", expression(new FieldReference(new FieldReference(new Reference(nestedRowType, "a"), 0), 0)),
                                                "expr_0", expression(new FieldReference(new Reference(ROW_TYPE, "b"), 1)),
                                                "expr_1", expression(new FieldReference(new Reference(nestedRowType, "a"), 0)),
                                                "a", expression(new Reference(nestedRowType, "a")),
                                                "b", expression(new Reference(ROW_TYPE, "b"))),
                                        tableScan(
                                                testTable.connectorHandle()::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of(
                                                        "a", new TpchColumnHandle("a", nestedRowType)::equals,
                                                        "b", new TpchColumnHandle("b", ROW_TYPE)::equals))))));
    }

    @Test
    public void testPushdownDereferenceThroughFilter()
    {
        tester().assertThat(new PushDownDereferenceThroughFilter())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", BIGINT), new FieldReference(new Reference(ROW_TYPE, "msg"), 0),
                                        p.symbol("expr_2", BIGINT), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0)),
                                p.filter(
                                        new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new FieldReference(new Reference(ROW_TYPE, "msg"), 0), new Constant(BIGINT, 3L)), new Not(new IsNull(new Reference(ROW_TYPE, "msg2"))))),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr", expression(new Reference(BIGINT, "msg_x")),
                                        "expr_2", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))), // not pushed down since predicate contains msg2 reference
                                filter(
                                        new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(BIGINT, "msg_x"), new Constant(BIGINT, 3L)), new Not(new IsNull(new Reference(ROW_TYPE, "msg2"))))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg"), 0)),
                                                        "msg", expression(new Reference(ROW_TYPE, "msg")),
                                                        "msg2", expression(new Reference(ROW_TYPE, "msg2"))),
                                                values("msg", "msg2")))));
    }

    @Test
    public void testPushDownDereferenceThroughLimit()
    {
        tester().assertThat(new PushDownDereferencesThroughLimit())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_y"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 1))
                                        .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                        .build(),
                                p.limit(10,
                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new Reference(BIGINT, "x")))
                                        .put("msg2_y", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 1)))
                                        .put("z", expression(new Reference(BIGINT, "z")))
                                        .buildOrThrow(),
                                limit(
                                        10,
                                        ImmutableList.of(sort("msg2", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                                        .put("z", expression(new Reference(BIGINT, "z")))
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2", "z")))));
    }

    @Test
    public void testPushDownDereferenceThroughLimitWithPreSortedInputs()
    {
        tester().assertThat(new PushDownDereferencesThroughLimit())
                .on(p -> p.project(
                        Assignments.builder()
                                .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                .put(p.symbol("msg2_y"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 1))
                                .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                .build(),
                        p.limit(
                                10,
                                false,
                                ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new Reference(BIGINT, "x")))
                                        .put("msg2_y", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 1)))
                                        .put("z", expression(new Reference(BIGINT, "z")))
                                        .buildOrThrow(),
                                limit(
                                        10,
                                        ImmutableList.of(),
                                        false,
                                        ImmutableList.of("msg2"),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                                        .put("z", expression(new Reference(BIGINT, "z")))
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2", "z")))));
    }

    @Test
    public void testPushDownDereferenceThroughSort()
    {
        // Does not fire if symbols are used in the ordering scheme
        tester().assertThat(new PushDownDereferencesThroughSort())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), new FieldReference(new Reference(ROW_TYPE, "msg"), 0))
                                        .put(p.symbol("msg_y"), new FieldReference(new Reference(ROW_TYPE, "msg"), 1))
                                        .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                        .build(),
                                p.sort(
                                        ImmutableList.of(p.symbol("z"), p.symbol("msg", ROW_TYPE)),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("z")))))
                .doesNotFire();

        tester().assertThat(new PushDownDereferencesThroughSort())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), new FieldReference(new Reference(ROW_TYPE, "msg"), 0))
                                        .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                        .build(),
                                p.sort(
                                        ImmutableList.of(p.symbol("z")),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg_x", expression(new Reference(BIGINT, "x")))
                                        .put("z", expression(new Reference(BIGINT, "z")))
                                        .buildOrThrow(),
                                sort(ImmutableList.of(sort("z", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", expression(new FieldReference(new Reference(ROW_TYPE, "msg"), 0)))
                                                        .put("z", expression(new Reference(BIGINT, "z")))
                                                        .put("msg", expression(new Reference(ROW_TYPE, "msg")))
                                                        .buildOrThrow(),
                                                values("msg", "z")))));
    }

    @Test
    public void testPushdownDereferenceThroughRowNumber()
    {
        tester().assertThat(new PushDownDereferencesThroughRowNumber())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .build(),
                                p.rowNumber(
                                        ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                        Optional.empty(),
                                        p.symbol("row_number"),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                        .put("msg2_x", expression(new Reference(BIGINT, "expr")))
                                        .buildOrThrow(),
                                rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("msg1")),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0)))
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testPushdownDereferenceThroughTopNRanking()
    {
        tester().assertThat(new PushDownDereferencesThroughTopNRanking())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .put(p.symbol("msg3_x"), new FieldReference(new Reference(ROW_TYPE, "msg3"), 0))
                                        .build(),
                                p.topNRanking(
                                        new DataOrganizationSpecification(
                                                ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                                Optional.of(new OrderingScheme(
                                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                                        ImmutableMap.of(p.symbol("msg2", ROW_TYPE), ASC_NULLS_FIRST)))),
                                        ROW_NUMBER,
                                        5,
                                        p.symbol("ranking"),
                                        Optional.empty(),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE), p.symbol("msg3", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                        .put("msg2_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0)))
                                        .put("msg3_x", expression(new Reference(BIGINT, "expr")))
                                        .buildOrThrow(),
                                topNRanking(
                                        pattern -> pattern.specification(singletonList("msg1"), singletonList("msg2"), ImmutableMap.of("msg2", ASC_NULLS_FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg3"), 0)))
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .put("msg3", expression(new Reference(ROW_TYPE, "msg3")))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2", "msg3")))));
    }

    @Test
    public void testPushdownDereferenceThroughTopN()
    {
        tester().assertThat(new PushDownDereferencesThroughTopN())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .build(),
                                p.topN(5, ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                        .put("msg2_x", expression(new Reference(BIGINT, "expr")))
                                        .buildOrThrow(),
                                topN(5, ImmutableList.of(sort("msg1", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0)))
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testPushdownDereferenceThroughWindow()
    {
        tester().assertThat(new PushDownDereferencesThroughWindow())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .put(p.symbol("msg3_x"), new FieldReference(new Reference(ROW_TYPE, "msg3"), 0))
                                        .put(p.symbol("msg4_x"), new FieldReference(new Reference(ROW_TYPE, "msg4"), 0))
                                        .put(p.symbol("msg5_x"), new FieldReference(new Reference(ROW_TYPE, "msg5"), 0))
                                        .build(),
                                p.window(
                                        new DataOrganizationSpecification(
                                                ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                                Optional.of(new OrderingScheme(
                                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                                        ImmutableMap.of(p.symbol("msg2", ROW_TYPE), ASC_NULLS_FIRST)))),
                                        ImmutableMap.of(
                                                p.symbol("msg6", ROW_TYPE),
                                                // min function on MSG_TYPE
                                                new WindowNode.Function(
                                                        createTestMetadataManager().resolveBuiltinFunction("min", fromTypes(ROW_TYPE)),
                                                        ImmutableList.of(p.symbol("msg3", ROW_TYPE).toSymbolReference()),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                UNBOUNDED_PRECEDING,
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                CURRENT_ROW,
                                                                Optional.empty(),
                                                                Optional.empty()),
                                                        true)),
                                        p.values(
                                                p.symbol("msg1", ROW_TYPE),
                                                p.symbol("msg2", ROW_TYPE),
                                                p.symbol("msg3", ROW_TYPE),
                                                p.symbol("msg4", ROW_TYPE),
                                                p.symbol("msg5", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))) // not pushed down because used in partitionBy
                                        .put("msg2_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))) // not pushed down because used in orderBy
                                        .put("msg3_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg3"), 0))) // not pushed down because the whole column is used in windowNode function
                                        .put("msg4_x", expression(new Reference(BIGINT, "expr"))) // pushed down because msg4[1] is being used in the function
                                        .put("msg5_x", expression(new Reference(BIGINT, "expr2"))) // pushed down because not referenced in windowNode
                                        .buildOrThrow(),
                                window(
                                        windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(singletonList("msg1"), singletonList("msg2"), ImmutableMap.of("msg2", SortOrder.ASC_NULLS_FIRST))
                                                .addFunction(windowFunction("min", singletonList("msg3"), DEFAULT_FRAME)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .put("msg3", expression(new Reference(ROW_TYPE, "msg3")))
                                                        .put("msg4", expression(new Reference(ROW_TYPE, "msg4")))
                                                        .put("msg5", expression(new Reference(ROW_TYPE, "msg5")))
                                                        .put("expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg4"), 0)))
                                                        .put("expr2", expression(new FieldReference(new Reference(ROW_TYPE, "msg5"), 0)))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2", "msg3", "msg4", "msg5")))));
    }

    @Test
    public void testPushdownDereferenceThroughAssignUniqueId()
    {
        tester().assertThat(new PushDownDereferencesThroughAssignUniqueId())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("expr"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .build(),
                                p.assignUniqueId(
                                        p.symbol("unique"),
                                        p.values(p.symbol("msg1", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", expression(new Reference(BIGINT, "msg1_x"))),
                                assignUniqueId(
                                        "unique",
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg1_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                                        .buildOrThrow(),
                                                values("msg1")))));
    }

    @Test
    public void testPushdownDereferenceThroughMarkDistinct()
    {
        tester().assertThat(new PushDownDereferencesThroughMarkDistinct())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), new FieldReference(new Reference(ROW_TYPE, "msg1"), 0))
                                        .put(p.symbol("msg2_x"), new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))
                                        .build(),
                                p.markDistinct(
                                        p.symbol("is_distinct", BOOLEAN),
                                        singletonList(p.symbol("msg2", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "msg1_x", expression(new Reference(BIGINT, "expr")), // pushed down
                                        "msg2_x", expression(new FieldReference(new Reference(ROW_TYPE, "msg2"), 0))),   // not pushed down because used in markDistinct
                                markDistinct(
                                        "is_distinct",
                                        singletonList("msg2"),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", expression(new Reference(ROW_TYPE, "msg1")))
                                                        .put("msg2", expression(new Reference(ROW_TYPE, "msg2")))
                                                        .put("expr", expression(new FieldReference(new Reference(ROW_TYPE, "msg1"), 0)))
                                                        .buildOrThrow(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testMultiLevelPushdown()
    {
        RowType complexType = rowType(field("f1", rowType(field("f1", BIGINT), field("f2", BIGINT))), field("f2", BIGINT));
        tester().assertThat(new PushDownDereferenceThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1", complexType.getFields().get(0).getType()), new FieldReference(new Reference(complexType, "a"), 0),
                                        p.symbol("expr_2"), new Call(
                                                ADD_BIGINT,
                                                ImmutableList.of(
                                                        new Call(
                                                                ADD_BIGINT,
                                                                ImmutableList.of(
                                                                        new Call(
                                                                                ADD_BIGINT,
                                                                                ImmutableList.of(
                                                                                        new FieldReference(new FieldReference(new Reference(complexType, "a"), 0), 0),
                                                                                        new Constant(BIGINT, 2L))),
                                                                        new FieldReference(
                                                                                new FieldReference(new Reference(complexType, "b"), 0), 0))),
                                                        new FieldReference(
                                                                new FieldReference(
                                                                        new Reference(complexType, "b"),
                                                                        0),
                                                                1)))),
                                p.project(
                                        Assignments.identity(ImmutableList.of(p.symbol("a", complexType), p.symbol("b", complexType))),
                                        p.values(p.symbol("a", complexType), p.symbol("b", complexType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr_1", expression(new Reference(complexType.getFields().get(0).getType(), "a_f1")),
                                        "expr_2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Call(ADD_BIGINT, ImmutableList.of(new Call(ADD_BIGINT, ImmutableList.of(new FieldReference(new Reference(complexType.getFields().get(0).getType(), "a_f1"), 0), new Constant(BIGINT, 2L))), new Reference(BIGINT, "b_f1_f1"))), new Reference(BIGINT, "b_f1_f2"))))),
                                strictProject(
                                        ImmutableMap.of(
                                                "a", expression(new Reference(complexType, "a")),
                                                "b", expression(new Reference(complexType, "b")),
                                                "a_f1", expression(new FieldReference(new Reference(complexType, "a"), 0)),
                                                "b_f1_f1", expression(new FieldReference(new FieldReference(new Reference(complexType, "b"), 0), 0)),
                                                "b_f1_f2", expression(new FieldReference(new FieldReference(new Reference(complexType, "b"), 0), 1))),
                                        values("a", "b"))));
    }
}
