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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_PUSH_FIELD_DEREFERENCE_LAMBDA_INTO_SCAN;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownSubscriptLambdaRules
        extends BaseRuleTest
{
    private static final Type ROW_TYPE = RowType.anonymous(ImmutableList.of(BIGINT, BIGINT));
    private static final Type PRUNED_ROW_TYPE = RowType.anonymous(ImmutableList.of(BIGINT));
    private static final Type ARRAY_ROW_TYPE = new ArrayType(ROW_TYPE);
    private static final Type PRUNED_ARRAY_ROW_TYPE = new ArrayType(PRUNED_ROW_TYPE);
    private static final Reference LAMBDA_ELEMENT_REFERENCE = new Reference(ROW_TYPE, "transformarray$element");
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(ARRAY_ROW_TYPE, new FunctionType(ImmutableList.of(ROW_TYPE), PRUNED_ROW_TYPE)));
    private static final Call dereferenceFunctionCall = new Call(TRANSFORM, ImmutableList.of(new Reference(ARRAY_ROW_TYPE, "array_of_struct"),
            new Lambda(ImmutableList.of(new Symbol(ROW_TYPE, "transformarray$element")),
                    new Row(ImmutableList.of(new FieldReference(
                    LAMBDA_ELEMENT_REFERENCE,
                    0))))));

    @Test
    public void testPushDownSubscriptLambdaThroughProject()
    {
        PushDownFieldReferenceLambdaThroughProject pushDownFieldReferenceLambdaThroughProject = new PushDownFieldReferenceLambdaThroughProject();

        try (RuleTester ruleTester = RuleTester.builder().addSessionProperty(ENABLE_PUSH_FIELD_DEREFERENCE_LAMBDA_INTO_SCAN, "true").build()) {
            // Base symbol referenced by other assignments, skip the optimization
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughProject)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall, arrayOfRow, new Reference(ARRAY_ROW_TYPE, "array_of_struct")),
                                p.project(
                                        Assignments.identity(arrayOfRow),
                                        p.values(arrayOfRow)));
                    }).doesNotFire();

            // Dereference Lambda being pushed down to the lower projection
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughProject)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall),
                                p.project(
                                        Assignments.identity(arrayOfRow),
                                        p.values(arrayOfRow)));
                    })
                    .matches(
                            project(
                                    ImmutableMap.of("pruned_nested_array", expression(new Reference(PRUNED_ARRAY_ROW_TYPE, "expr"))),
                                    project(
                                            ImmutableMap.of("expr", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                            values("array_of_struct"))));

            // Dereference Lambda being pushed down to the lower projection, with other symbols kept in projections
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughProject)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        Symbol e = p.symbol("e", ARRAY_ROW_TYPE);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall, e, new Reference(ARRAY_ROW_TYPE, "e")),
                                p.project(
                                        Assignments.identity(arrayOfRow, e),
                                        p.values(arrayOfRow, e)));
                    })
                    .matches(
                            project(
                                    ImmutableMap.of("pruned_nested_array", expression(new Reference(PRUNED_ARRAY_ROW_TYPE, "expr")), "e", expression(new Reference(ARRAY_ROW_TYPE, "e"))),
                                    project(
                                            ImmutableMap.of("expr", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build())), "e", expression(new Reference(ARRAY_ROW_TYPE, "e"))),
                                            values("array_of_struct", "e"))));
        }
    }

    @Test
    public void testPushDownSubscriptLambdaThroughFilter()
    {
        PushDownFieldReferenceLambdaThroughFilter pushDownFieldReferenceLambdaThroughFilter = new PushDownFieldReferenceLambdaThroughFilter();

        try (RuleTester ruleTester = RuleTester.builder().addSessionProperty(ENABLE_PUSH_FIELD_DEREFERENCE_LAMBDA_INTO_SCAN, "true").build()) {
            // Base symbol referenced by other assignments, skip the optimization
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughFilter)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        Symbol e = p.symbol("e", BIGINT);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall, arrayOfRow, new Reference(ARRAY_ROW_TYPE, "array_of_struct")),
                                p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                        p.project(
                                                Assignments.identity(arrayOfRow, e),
                                                p.values(arrayOfRow, e))));
                    }).doesNotFire();

            // No filter node, skip the rule
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughFilter)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall),
                                p.project(
                                        Assignments.identity(arrayOfRow),
                                        p.values(arrayOfRow)));
                    }).doesNotFire();

            // Base symbol referenced by predicate in filter node, skip the optimization
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughFilter)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall),
                                p.filter(new Comparison(NOT_EQUAL, new Reference(ARRAY_ROW_TYPE, "array_of_struct"), new Constant(ARRAY_ROW_TYPE, null)),
                                        p.project(
                                                Assignments.identity(arrayOfRow),
                                                p.values(arrayOfRow))));
                    }).doesNotFire();

            // FieldDereference Lambda being pushed down to the lower projection through filter, with other symbols kept in projections
            ruleTester.assertThat(pushDownFieldReferenceLambdaThroughFilter)
                    .on(p -> {
                        Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                        Symbol e = p.symbol("e", BIGINT);
                        return p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall, e, new Reference(BIGINT, "e")),
                                p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                        p.project(
                                                Assignments.identity(arrayOfRow, e),
                                                p.values(arrayOfRow, e))));
                    })
                    .matches(
                            project(
                                    ImmutableMap.of("pruned_nested_array", expression(new Reference(ARRAY_ROW_TYPE, "expr")), "e", expression(new Reference(BIGINT, "e"))),
                                    filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                            project(
                                                    ImmutableMap.of("expr", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build())), "e", expression(new Reference(BIGINT, "e"))),
                                                    project(
                                                            ImmutableMap.of("array_of_struct", expression(new Reference(ARRAY_ROW_TYPE, "array_of_struct")), "e", expression(new Reference(BIGINT, "e"))),
                                                            values("array_of_struct", "e"))))));
        }
    }
}
