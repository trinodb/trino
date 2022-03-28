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
import io.trino.operator.aggregation.arrayagg.ArrayAggregationFunction;
import io.trino.operator.scalar.ArrayDistinctFunction;
import io.trino.spi.type.ArrayType;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;

public class TestArrayAggregationAfterDistinct
        extends BaseRuleTest
{
    private static final QualifiedName ARRAY_AGG = createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of(ArrayAggregationFunction.NAME), fromTypes(BIGINT)).toQualifiedName();
    private static final QualifiedName ARRAY_MIN = createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("array_min"), fromTypes(new ArrayType(BIGINT))).toQualifiedName();
    private static final QualifiedName ARRAY_MAX = createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("array_max"), fromTypes(new ArrayType(BIGINT))).toQualifiedName();
    private static final QualifiedName ARRAY_DISTINCT = createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of(ArrayDistinctFunction.NAME), fromTypes(new ArrayType(BIGINT))).toQualifiedName();

    @Test
    public void singleArrayDistinctArrayAggRuleFires()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("array_agg", true, ImmutableList.of(anySymbol()));
        // SELECT ARRAY_DISTINCT(ARRAY_AGG(a)) FROM (VALUES (1), (1)) t(a)
        tester().assertThat(new ArrayAggregationAfterDistinct(tester().getMetadata()))
                .on(p -> {
                    SymbolReference arrayAgg = new SymbolReference("array_agg");
                    FunctionCall arrayDistinctAfterArrayAgg = new FunctionCall(
                            ARRAY_DISTINCT,
                            ImmutableList.of(arrayAgg));

                    return p.project(Assignments.builder()
                                    .put(p.symbol("output1"), arrayDistinctAfterArrayAgg)
                                    .build(),
                            p.aggregation(ap -> ap.globalGrouping()
                                    .step(PARTIAL)
                                    .addAggregation(p.symbol("array_agg"), expression("array_agg(a)"), ImmutableList.of(BIGINT))
                                    .source(p.values(p.symbol("a")))));
                })
                .matches(
                        project(ImmutableMap.of("output1", PlanMatchPattern.expression("array_agg")),
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("array_agg"), aggregationPattern),
                                        Optional.empty(),
                                        PARTIAL,
                                        node(GroupReference.class))));
    }

    @Test
    public void singleArrayDistinctArrayAggAlreadyDistinctRuleFires()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("array_agg", true, ImmutableList.of(anySymbol()));
        // SELECT ARRAY_DISTINCT(ARRAY_AGG(DISTINCT a)) FROM (VALUES (1), (1)) t(a)
        tester().assertThat(new ArrayAggregationAfterDistinct(tester().getMetadata()))
                .on(p -> {
                    SymbolReference arrayAgg = new SymbolReference("array_agg");
                    FunctionCall arrayDistinctAfterArrayAgg = new FunctionCall(
                            ARRAY_DISTINCT,
                            ImmutableList.of(arrayAgg));

                    return p.project(Assignments.builder()
                                    .put(p.symbol("output1"), arrayDistinctAfterArrayAgg)
                                    .build(),
                            p.aggregation(ap -> ap.globalGrouping()
                                    .step(PARTIAL)
                                    .addAggregation(p.symbol("array_agg"), expression("array_agg(distinct a)"), ImmutableList.of(BIGINT))
                                    .source(p.values(p.symbol("a")))));
                })
                .matches(
                        project(ImmutableMap.of("output1", PlanMatchPattern.expression("array_agg")),
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("array_agg"), aggregationPattern),
                                        Optional.empty(),
                                        PARTIAL,
                                        node(GroupReference.class))));
    }
//    Fails on java.lang.IllegalStateException: Ambiguous expression "array_agg" matches multiple assignments ["array_agg", "array_agg"]
//    @Test
//    public void multipleArrayDistinctSingleArrayAgg()
//    {
//        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("array_agg", true, ImmutableList.of(anySymbol()));
//        // SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_DISTINCT(ARRAY_AGG(a)) FROM (VALUES (1), (1)) t(a)
//        tester().assertThat(new ArrayAggregationAfterDistinct(tester().getMetadata()))
//                .on(p -> {
//                    SymbolReference array_agg = new SymbolReference("array_agg");
//                    FunctionCall arrayDistinctAfterArrayAgg = new FunctionCall(
//                            ARRAY_DISTINCT,
//                            ImmutableList.of(array_agg));
//
//                    return p.project(Assignments.builder()
//                                    .put(p.symbol("output1"), arrayDistinctAfterArrayAgg)
//                                    .put(p.symbol("output2"), arrayDistinctAfterArrayAgg)
//                                    .build(),
//                            p.aggregation(ap -> ap.globalGrouping()
//                                    .step(PARTIAL)
//                                    .addAggregation(p.symbol("array_agg"), expression("array_agg(a)"), ImmutableList.of(BIGINT))
//                                    .source(p.values(p.symbol("a")))));
//                })
//                .matches(
//                        project(ImmutableMap.of(
//                                "output1", PlanMatchPattern.expression("array_agg"),
//                                "output2", PlanMatchPattern.expression("array_agg")),
//                                aggregation(
//                                        globalAggregation(),
//                                        ImmutableMap.of(Optional.of("array_agg"), aggregationPattern),
//                                        Optional.empty(),
//                                        PARTIAL,
//                                        node(GroupReference.class))));
//    }

    @Test
    public void singleArrayDistinctArrayAggMultipleAssignmentsRuleDoesNotFire()
    {
        // SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_AGG(a) FROM (VALUES (1), (1)) t(a)
        tester().assertThat(new ArrayAggregationAfterDistinct(tester().getMetadata()))
                .on(p -> {
                    FunctionCall arrayAgg = new FunctionCall(
                            ARRAY_AGG,
                            ImmutableList.of(PlanBuilder.expression("a")));
                    FunctionCall arrayDistinctAfterArrayAgg = new FunctionCall(
                            ARRAY_DISTINCT,
                            ImmutableList.of(arrayAgg));
                    return p.project(Assignments.builder()
                                    .put(p.symbol("output1"), arrayDistinctAfterArrayAgg)
                                    .put(p.symbol("output2"), arrayAgg)
                                    .build(),
                            p.aggregation(ap -> ap.globalGrouping()
                                    .step(PARTIAL)
                                    .addAggregation(
                                            p.symbol("array_agg"),
                                            expression("array_agg(a)"),
                                            ImmutableList.of(BIGINT))
                                    .source(p.values(p.symbol("a")))));
                })
                .doesNotFire();
    }

    @Test
    public void deeplyNestedReferenceToArrayAggWithOtherSymbolsRuleDoesNotFire()
    {
        // SELECT ARRAY_DISTINCT(ARRAY_AGG(a)), ARRAY_MIN(ARRAY_AGG(a)) + ARRAY_MAX(ARRAY_AGG(b)) FROM (VALUES (1, 2), (1, 2)) t(a, b)
        tester().assertThat(new ArrayAggregationAfterDistinct(tester().getMetadata()))
                .on(p -> {
                    FunctionCall arrayDistinctAfterArrayAgg = new FunctionCall(
                            ARRAY_DISTINCT,
                            ImmutableList.of(new SymbolReference("array_agg")));
                    FunctionCall arrayMinAfterArrayAgg = new FunctionCall(
                            ARRAY_MIN,
                            ImmutableList.of(new SymbolReference("array_agg")));
                    FunctionCall arrayMaxAfterArrayAgg = new FunctionCall(
                            ARRAY_MAX,
                            ImmutableList.of(new SymbolReference("array_agg")));
                    ArithmeticBinaryExpression nestedArrayAggReferences = new ArithmeticBinaryExpression(
                            ADD,
                            arrayMinAfterArrayAgg,
                            arrayMaxAfterArrayAgg);

                    return p.project(Assignments.builder()
                                    .put(p.symbol("output1"), arrayDistinctAfterArrayAgg)
                                    .put(p.symbol("output2"), nestedArrayAggReferences)
                                    .build(),
                            p.aggregation(ap -> ap.globalGrouping()
                                    .step(PARTIAL)
                                    .addAggregation(
                                            p.symbol("array_agg"),
                                            expression("array_agg(a)"),
                                            ImmutableList.of(BIGINT))
                                    .source(p.values(p.symbol("a")))));
                }).doesNotFire();
    }
}
