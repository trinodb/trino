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
import io.prestosql.spi.block.MethodHandleUtil;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.UnnestNode;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneUnnestMapping
        extends BaseRuleTest
{
    private static final RowType simpleRowType = rowType(field("f1", BIGINT), field("f2", BIGINT));
    private static final RowType nestedRowType = rowType(field("f1", BIGINT), field("f2", simpleRowType));
    private static final ArrayType nestedArrayType = new ArrayType(nestedRowType);

    @Test
    public void testDoesNotFire()
    {
        MapType nestedMapType = new MapType(
                BIGINT,
                nestedRowType,
                MethodHandleUtil.methodHandle(TestPruneUnnestMapping.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPruneUnnestMapping.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPruneUnnestMapping.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPruneUnnestMapping.class, "throwUnsupportedOperation"));

        // Does not fire for maps
        tester().assertThat(new PruneUnnestMappings(tester().getTypeAnalyzer(), tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("expr", BIGINT), expression("unnested_value.f2")),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", BIGINT)),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(
                                                        p.symbol("nested_map", nestedMapType),
                                                        ImmutableList.of(p.symbol("unnested_key", BIGINT), p.symbol("unnested_value", nestedRowType)))),
                                        p.values(
                                                p.symbol("replicate", BIGINT),
                                                p.symbol("nested_map", nestedMapType)))))
                .doesNotFire();

        // Does not fire if unnest symbol is also used as replicate symbol
        tester().assertThat(new PruneUnnestMappings(tester().getTypeAnalyzer(), tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("expr", BIGINT), expression("unnested_row.f2"))
                                        .build(),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", BIGINT), p.symbol("nested_array", nestedArrayType)),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(
                                                        p.symbol("nested_array", nestedArrayType),
                                                        ImmutableList.of(p.symbol("unnested_bigint", BIGINT), p.symbol("unnested_row", nestedRowType)))),
                                        p.values(
                                                p.symbol("replicate", BIGINT),
                                                p.symbol("nested_array", nestedArrayType)))))
                .doesNotFire();
    }

    @Test
    public void testSimple()
    {
        // Test with dereferences on unnested column
        tester().assertThat(new PruneUnnestMappings(tester().getTypeAnalyzer(), tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("expr", BIGINT), expression("unnested_row.f2"))
                                        .build(),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", BIGINT)),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(
                                                        p.symbol("nested_array", nestedArrayType),
                                                        ImmutableList.of(p.symbol("unnested_bigint", BIGINT), p.symbol("unnested_row", nestedRowType)))),
                                        p.values(
                                                p.symbol("replicate", BIGINT),
                                                p.symbol("nested_array", nestedArrayType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr", PlanMatchPattern.expression("unnested_row_f2")),
                                unnest(
                                        ImmutableList.of("replicate"),
                                        ImmutableList.of(unnestMapping("nested_array_transformed", ImmutableList.of("unnested_row_f2"))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "nested_array_transformed", PlanMatchPattern.expression("transform(nested_array, x -> x.f2.f2)"),
                                                        "replicate", PlanMatchPattern.expression("replicate")),
                                                values("replicate", "nested_array")))));
    }

    @Test
    public void testMultipleMappings()
    {
        // Test with dereferences on unnested column
        tester().assertThat(new PruneUnnestMappings(tester().getTypeAnalyzer(), tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", BIGINT), expression("row_1.f2"),
                                        p.symbol("expr_2", BIGINT), expression("bigint_1")),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", BIGINT)),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(
                                                        p.symbol("array_1", nestedArrayType),
                                                        ImmutableList.of(p.symbol("bigint_1", BIGINT), p.symbol("row_1", nestedRowType))),
                                                new UnnestNode.Mapping(
                                                        p.symbol("array_2", nestedArrayType),
                                                        ImmutableList.of(p.symbol("bigint_2", BIGINT), p.symbol("row_2", nestedRowType)))),
                                        p.values(
                                                p.symbol("replicate", BIGINT),
                                                p.symbol("array_1", nestedArrayType),
                                                p.symbol("array_2", nestedArrayType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr", PlanMatchPattern.expression("unnested_row_f2"),
                                        "expr_2", PlanMatchPattern.expression("unnested_bigint")),
                                unnest(
                                        ImmutableList.of("replicate"),
                                        ImmutableList.of(
                                                unnestMapping("transformed_array_1", ImmutableList.of("unnested_bigint", "unnested_row_f2")),
                                                unnestMapping("transformed_array_2", ImmutableList.of("unused_symbol"))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "replicate", PlanMatchPattern.expression("replicate"),
                                                        "transformed_array_1", PlanMatchPattern.expression("transform(array_1, x -> ROW(x.f1, x.f2.f2))"),
                                                        "transformed_array_2", PlanMatchPattern.expression("transform(array_2, x -> x.f1)")),
                                                values("replicate", "array_1", "array_2")))));
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
