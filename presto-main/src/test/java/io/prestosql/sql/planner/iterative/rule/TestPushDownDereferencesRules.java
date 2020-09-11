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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.sort;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topN;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.iterative.rule.test.RuleTester.CATALOG_ID;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.tree.SortItem.NullOrdering.FIRST;
import static io.prestosql.sql.tree.SortItem.Ordering.ASCENDING;
import static java.util.Collections.singletonList;

public class TestPushDownDereferencesRules
        extends BaseRuleTest
{
    private static final RowType ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

    @Test
    public void testDoesNotFire()
    {
        // rule does not fire for symbols
        tester().assertThat(new PushDownDereferenceThroughFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("x > BIGINT '5'"),
                                p.values(p.symbol("x"))))
                .doesNotFire();

        // Pushdown is not enabled if dereferences come from an expression that is not a simple dereference chain
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1"), expression("cast(row(a, b) as row(f1 row(x bigint, y bigint), f2 bigint)).f1"),
                                        p.symbol("expr_2"), expression("cast(row(a, b) as row(f1 row(x bigint, y bigint), f2 bigint)).f1.y")),
                                p.project(
                                        Assignments.of(
                                                p.symbol("a", ROW_TYPE), expression("a"),
                                                p.symbol("b"), expression("b")),
                                        p.values(p.symbol("a", ROW_TYPE), p.symbol("b")))))
                .doesNotFire();

        // Does not fire when base symbols are referenced along with the dereferences
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("expr", ROW_TYPE), expression("a"), p.symbol("a_x"), expression("a.x")),
                                p.project(
                                        Assignments.of(p.symbol("a", ROW_TYPE), expression("a")),
                                        p.values(p.symbol("a", ROW_TYPE)))))
                .doesNotFire();
    }

    @Test
    public void testPushdownDereferenceThroughProject()
    {
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                                p.project(
                                        Assignments.of(
                                                p.symbol("y"), expression("y"),
                                                p.symbol("msg", ROW_TYPE), expression("msg")),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("y")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                                strictProject(
                                        ImmutableMap.of(
                                                "msg_x", PlanMatchPattern.expression("msg.x"),
                                                "y", PlanMatchPattern.expression("y"),
                                                "msg", PlanMatchPattern.expression("msg")),
                                        values("msg", "y"))));
    }

    @Test
    public void testPushDownDereferenceThroughJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), expression("msg1.x"))
                                        .put(p.symbol("right_y"), expression("msg2.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("unreferenced_symbol")),
                                        p.values(p.symbol("msg2", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", PlanMatchPattern.expression("x"))
                                        .put("right_y", PlanMatchPattern.expression("y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                join(INNER, ImmutableList.of(),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "x", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1"),
                                                        "unreferenced_symbol", PlanMatchPattern.expression("unreferenced_symbol")),
                                                values("msg1", "unreferenced_symbol")),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("y", PlanMatchPattern.expression("msg2.y"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .build(),
                                                values("msg2", "z")))));

        // Verify pushdown for filters
        tester().assertThat(new PushDownDereferenceThroughJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr"), expression("msg1.x"),
                                        p.symbol("expr_2"), expression("msg2")),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", ROW_TYPE)),
                                        p.values(p.symbol("msg2", ROW_TYPE)),
                                        p.expression("msg1.x + msg2.y > BIGINT '10'"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "expr", PlanMatchPattern.expression("msg1_x"),
                                        "expr_2", PlanMatchPattern.expression("msg2")),
                                join(INNER, ImmutableList.of(), Optional.of("msg1_x + msg2.y > BIGINT '10'"),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg1_x", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1")),
                                                values("msg1")),
                                        values("msg2"))));
    }

    @Test
    public void testPushdownDereferencesThroughSemiJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughSemiJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
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
                                        .put("msg1_x", PlanMatchPattern.expression("expr"))
                                        .put("msg2_x", PlanMatchPattern.expression("msg2.x"))   // Not pushed down because msg2 is sourceJoinSymbol
                                        .build(),
                                semiJoin(
                                        "msg2",
                                        "filtering_msg",
                                        "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "expr", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1"),
                                                        "msg2", PlanMatchPattern.expression("msg2")),
                                                values("msg1", "msg2")),
                                        values("filtering_msg"))));
    }

    @Test
    public void testPushdownDereferencesThroughUnnest()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        tester().assertThat(new PushDownDereferenceThroughUnnest(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                                p.unnest(
                                        ImmutableList.of(p.symbol("msg", ROW_TYPE)),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("arr", arrayType), ImmutableList.of(p.symbol("field")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("arr", arrayType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                                unnest(
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg_x", PlanMatchPattern.expression("msg.x"),
                                                        "msg", PlanMatchPattern.expression("msg"),
                                                        "arr", PlanMatchPattern.expression("arr")),
                                                values("msg", "arr")))));

        // Test with dereferences on unnested column
        RowType rowType = rowType(field("f1", BIGINT), field("f2", BIGINT));
        ArrayType nestedColumnType = new ArrayType(rowType(field("f1", BIGINT), field("f2", rowType)));

        tester().assertThat(new PushDownDereferenceThroughUnnest(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("deref_replicate", BIGINT), expression("replicate.f2"),
                                        p.symbol("deref_unnest", BIGINT), expression("unnested_row.f2")),
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
                                        "deref_replicate", PlanMatchPattern.expression("symbol"),
                                        "deref_unnest", PlanMatchPattern.expression("unnested_row.f2")),    // not pushed down
                                unnest(
                                        ImmutableList.of("replicate", "symbol"),
                                        ImmutableList.of(unnestMapping("nested", ImmutableList.of("unnested_bigint", "unnested_row"))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "symbol", PlanMatchPattern.expression("replicate.f2"),
                                                        "replicate", PlanMatchPattern.expression("replicate"),
                                                        "nested", PlanMatchPattern.expression("nested")),
                                                values("replicate", "nested")))));
    }

    @Test
    public void testExtractDereferencesFromFilterAboveScan()
    {
        TableHandle testTable = new TableHandle(
                new CatalogName(CATALOG_ID),
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());

        RowType nestedRowType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("nested"), ROW_TYPE)));
        tester().assertThat(new ExtractDereferencesFromFilterAboveScan(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("a.nested.x != 5 AND b.y = 2 AND CAST(a.nested as JSON) is not null"),
                                p.tableScan(
                                        testTable,
                                        ImmutableList.of(p.symbol("a", nestedRowType), p.symbol("b", ROW_TYPE)),
                                        ImmutableMap.of(
                                                p.symbol("a", nestedRowType), new TpchColumnHandle("a", nestedRowType),
                                                p.symbol("b", ROW_TYPE), new TpchColumnHandle("b", ROW_TYPE)))))
                .matches(project(
                        filter("expr != 5 AND expr_0 = 2 AND CAST(expr_1 as JSON) is not null",
                                strictProject(
                                        ImmutableMap.of(
                                                "expr", PlanMatchPattern.expression("a.nested.x"),
                                                "expr_0", PlanMatchPattern.expression("b.y"),
                                                "expr_1", PlanMatchPattern.expression("a.nested"),
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b")),
                                        tableScan(
                                                equalTo(testTable.getConnectorHandle()),
                                                TupleDomain.all(),
                                                ImmutableMap.of(
                                                        "a", equalTo(new TpchColumnHandle("a", nestedRowType)),
                                                        "b", equalTo(new TpchColumnHandle("b", ROW_TYPE))))))));
    }

    @Test
    public void testPushdownDereferenceThroughFilter()
    {
        tester().assertThat(new PushDownDereferenceThroughFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", BIGINT), expression("msg.x"),
                                        p.symbol("expr_2", BIGINT), expression("msg2.x")),
                                p.filter(
                                        expression("msg.x <> 'foo' AND msg2 is NOT NULL"),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr", PlanMatchPattern.expression("msg_x"),
                                        "expr_2", PlanMatchPattern.expression("msg2.x")), // not pushed down since predicate contains msg2 reference
                                filter(
                                        "msg_x <> 'foo' AND msg2 is NOT NULL",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg_x", PlanMatchPattern.expression("msg.x"),
                                                        "msg", PlanMatchPattern.expression("msg"),
                                                        "msg2", PlanMatchPattern.expression("msg2")),
                                                values("msg", "msg2")))));
    }

    @Test
    public void testPushDownDereferenceThroughLimit()
    {
        tester().assertThat(new PushDownDereferencesThroughLimit(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_y"), expression("msg2.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.limit(10,
                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", PlanMatchPattern.expression("x"))
                                        .put("msg2_y", PlanMatchPattern.expression("msg2.y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                limit(
                                        10,
                                        ImmutableList.of(sort("msg2", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", PlanMatchPattern.expression("msg1.x"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .build(),
                                                values("msg1", "msg2", "z")))));
    }

    @Test
    public void testPushDownDereferenceThroughSort()
    {
        // Does not fire if symbols are used in the ordering scheme
        tester().assertThat(new PushDownDereferencesThroughSort(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), expression("msg.x"))
                                        .put(p.symbol("msg_y"), expression("msg.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.sort(
                                        ImmutableList.of(p.symbol("z"), p.symbol("msg", ROW_TYPE)),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("z")))))
                .doesNotFire();

        tester().assertThat(new PushDownDereferencesThroughSort(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), expression("msg.x"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.sort(
                                        ImmutableList.of(p.symbol("z")),
                                        p.values(p.symbol("msg", ROW_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg_x", PlanMatchPattern.expression("x"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                sort(ImmutableList.of(sort("z", ASCENDING, SortItem.NullOrdering.FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", PlanMatchPattern.expression("msg.x"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .put("msg", PlanMatchPattern.expression("msg"))
                                                        .build(),
                                                values("msg", "z")))));
    }

    @Test
    public void testPushdownDereferenceThroughRowNumber()
    {
        tester().assertThat(new PushDownDereferencesThroughRowNumber(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
                                        .build(),
                                p.rowNumber(
                                        ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                        Optional.empty(),
                                        p.symbol("row_number"),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", PlanMatchPattern.expression("msg1.x"))
                                        .put("msg2_x", PlanMatchPattern.expression("expr"))
                                        .build(),
                                rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("msg1")),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", PlanMatchPattern.expression("msg2.x"))
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .build(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testPushdownDereferenceThroughTopNRowNumber()
    {
        tester().assertThat(new PushDownDereferencesThroughTopNRowNumber(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
                                        .put(p.symbol("msg3_x"), expression("msg3.x"))
                                        .build(),
                                p.topNRowNumber(
                                        new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                                Optional.of(new OrderingScheme(
                                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                                        ImmutableMap.of(p.symbol("msg2", ROW_TYPE), ASC_NULLS_FIRST)))),
                                        5,
                                        p.symbol("row_number"),
                                        Optional.empty(),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE), p.symbol("msg3", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", PlanMatchPattern.expression("msg1.x"))
                                        .put("msg2_x", PlanMatchPattern.expression("msg2.x"))
                                        .put("msg3_x", PlanMatchPattern.expression("expr"))
                                        .build(),
                                topNRowNumber(
                                        pattern -> pattern.specification(singletonList("msg1"), singletonList("msg2"), ImmutableMap.of("msg2", ASC_NULLS_FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", PlanMatchPattern.expression("msg3.x"))
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .put("msg3", PlanMatchPattern.expression("msg3"))
                                                        .build(),
                                                values("msg1", "msg2", "msg3")))));
    }

    @Test
    public void testPushdownDereferenceThroughTopN()
    {
        tester().assertThat(new PushDownDereferencesThroughTopN(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
                                        .build(),
                                p.topN(5, ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg1_x", PlanMatchPattern.expression("msg1.x"))
                                        .put("msg2_x", PlanMatchPattern.expression("expr"))
                                        .build(),
                                topN(5, ImmutableList.of(sort("msg1", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("expr", PlanMatchPattern.expression("msg2.x"))
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .build(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testPushdownDereferenceThroughWindow()
    {
        tester().assertThat(new PushDownDereferencesThroughWindow(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
                                        .put(p.symbol("msg3_x"), expression("msg3.x"))
                                        .put(p.symbol("msg4_x"), expression("msg4.x"))
                                        .put(p.symbol("msg5_x"), expression("msg5.x"))
                                        .build(),
                                p.window(
                                        new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("msg1", ROW_TYPE)),
                                                Optional.of(new OrderingScheme(
                                                        ImmutableList.of(p.symbol("msg2", ROW_TYPE)),
                                                        ImmutableMap.of(p.symbol("msg2", ROW_TYPE), ASC_NULLS_FIRST)))),
                                        ImmutableMap.of(
                                                p.symbol("msg6", ROW_TYPE),
                                                // min function on MSG_TYPE
                                                new WindowNode.Function(
                                                        createTestMetadataManager().resolveFunction(QualifiedName.of("min"), fromTypes(ROW_TYPE)),
                                                        ImmutableList.of(p.symbol("msg3", ROW_TYPE).toSymbolReference()),
                                                        new WindowNode.Frame(
                                                                WindowFrame.Type.RANGE,
                                                                FrameBound.Type.UNBOUNDED_PRECEDING,
                                                                Optional.empty(),
                                                                FrameBound.Type.UNBOUNDED_FOLLOWING,
                                                                Optional.empty(),
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
                                        .put("msg1_x", PlanMatchPattern.expression("msg1.x")) // not pushed down because used in partitionBy
                                        .put("msg2_x", PlanMatchPattern.expression("msg2.x")) // not pushed down because used in orderBy
                                        .put("msg3_x", PlanMatchPattern.expression("msg3.x")) // not pushed down because the whole column is used in windowNode function
                                        .put("msg4_x", PlanMatchPattern.expression("expr")) // pushed down because msg4.x is being used in the function
                                        .put("msg5_x", PlanMatchPattern.expression("expr2")) // pushed down because not referenced in windowNode
                                        .build(),
                                window(
                                        windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(singletonList("msg1"), singletonList("msg2"), ImmutableMap.of("msg2", SortOrder.ASC_NULLS_FIRST))
                                                .addFunction(functionCall("min", singletonList("msg3"))),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .put("msg3", PlanMatchPattern.expression("msg3"))
                                                        .put("msg4", PlanMatchPattern.expression("msg4"))
                                                        .put("msg5", PlanMatchPattern.expression("msg5"))
                                                        .put("expr", PlanMatchPattern.expression("msg4.x"))
                                                        .put("expr2", PlanMatchPattern.expression("msg5.x"))
                                                        .build(),
                                                values("msg1", "msg2", "msg3", "msg4", "msg5")))));
    }

    @Test
    public void testPushdownDereferenceThroughAssignUniqueId()
    {
        tester().assertThat(new PushDownDereferencesThroughAssignUniqueId(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("expr"), expression("msg1.x"))
                                        .build(),
                                p.assignUniqueId(
                                        p.symbol("unique"),
                                        p.values(p.symbol("msg1", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", PlanMatchPattern.expression("msg1_x")),
                                assignUniqueId(
                                        "unique",
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg1_x", PlanMatchPattern.expression("msg1.x"))
                                                        .build(),
                                                values("msg1")))));
    }

    @Test
    public void testPushdownDereferenceThroughMarkDistinct()
    {
        tester().assertThat(new PushDownDereferencesThroughMarkDistinct(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg1_x"), expression("msg1.x"))
                                        .put(p.symbol("msg2_x"), expression("msg2.x"))
                                        .build(),
                                p.markDistinct(
                                        p.symbol("is_distinct", BOOLEAN),
                                        singletonList(p.symbol("msg2", ROW_TYPE)),
                                        p.values(p.symbol("msg1", ROW_TYPE), p.symbol("msg2", ROW_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "msg1_x", PlanMatchPattern.expression("expr"), // pushed down
                                        "msg2_x", PlanMatchPattern.expression("msg2.x")),   // not pushed down because used in markDistinct
                                markDistinct(
                                        "is_distinct",
                                        singletonList("msg2"),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("msg1", PlanMatchPattern.expression("msg1"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .put("expr", PlanMatchPattern.expression("msg1.x"))
                                                        .build(),
                                                values("msg1", "msg2")))));
    }

    @Test
    public void testMultiLevelPushdown()
    {
        Type complexType = rowType(field("f1", rowType(field("f1", BIGINT), field("f2", BIGINT))), field("f2", BIGINT));
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1"), expression("a.f1"),
                                        p.symbol("expr_2"), expression("a.f1.f1 + 2 + b.f1.f1 + b.f1.f2")),
                                p.project(
                                        Assignments.identity(ImmutableList.of(p.symbol("a", complexType), p.symbol("b", complexType))),
                                        p.values(p.symbol("a", complexType), p.symbol("b", complexType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr_1", PlanMatchPattern.expression("a_f1"),
                                        "expr_2", PlanMatchPattern.expression("a_f1.f1 + 2 + b_f1_f1 + b_f1_f2")),
                                strictProject(
                                        ImmutableMap.of(
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b"),
                                                "a_f1", PlanMatchPattern.expression("a.f1"),
                                                "b_f1_f1", PlanMatchPattern.expression("b.f1.f1"),
                                                "b_f1_f2", PlanMatchPattern.expression("b.f1.f2")),
                                        values("a", "b"))));
    }
}
