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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_PUSH_SUBSCRIPT_LAMBDA_INTO_SCAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class TestPushDownSubscriptLambdaRules
        extends BaseRuleTest
{
    private static final String SCHEMA_NAME = "test_schema";
    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .setSystemProperty(ENABLE_PUSH_SUBSCRIPT_LAMBDA_INTO_SCAN, "true")
            .build();

    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));
    private static final Type PRUNED_ROW_TYPE = RowType.from(asList(field("a", BIGINT)));
    private static final Type ARRAY_PRUNED_ROW_TYPE = new ArrayType(PRUNED_ROW_TYPE);
    private static final Type ARRAY_ROW_TYPE = new ArrayType(ROW_TYPE);
    private static final FunctionCall subscriptfunctionCall = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
            .setName(QualifiedName.of("transform"))
            .addArgument(ARRAY_ROW_TYPE, new SymbolReference("array_of_struct"))
            .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                    new LambdaExpression(ImmutableList.of(
                            new LambdaArgumentDeclaration(
                                    new Identifier("transformarray$element"))),
                            new Row(ImmutableList.of(new SubscriptExpression(
                                    new SymbolReference("transformarray$element"),
                                    new LongLiteral("1"))))))
            .build();

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(HIVE_SESSION);
        return Optional.of(queryRunner);
    }

    @Test
    public void testPushDownSubscriptLambdaThroughProject()
    {
        PushDownSubscriptLambdaThroughProject pushDownSubscriptLambdaThroughProject = new PushDownSubscriptLambdaThroughProject(
                tester().getTypeAnalyzer());

        // Base symbol referenced by other assignments, skip the optimization
        tester().assertThat(pushDownSubscriptLambdaThroughProject)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall, arrayOfRow, new SymbolReference("array_of_struct")),
                            p.project(
                                    Assignments.identity(arrayOfRow),
                                    p.values(arrayOfRow)));
                }).doesNotFire();

        // Subscript Lambda being pushed down to the lower projection
        tester().assertThat(pushDownSubscriptLambdaThroughProject)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall),
                            p.project(
                                    Assignments.identity(arrayOfRow),
                                    p.values(arrayOfRow)));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression("expr")),
                                project(
                                        ImmutableMap.of("expr", expression(subscriptfunctionCall)),
                                        values("array_of_struct"))));

        // Subscript Lambda being pushed down to the lower projection, with other symbols kept in projections
        tester().assertThat(pushDownSubscriptLambdaThroughProject)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    Symbol e = p.symbol("e", ARRAY_ROW_TYPE);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall, e, new SymbolReference("e")),
                            p.project(
                                    Assignments.identity(arrayOfRow, e),
                                    p.values(arrayOfRow, e)));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression("expr"), "e", expression("e")),
                                project(
                                        ImmutableMap.of("expr", expression(subscriptfunctionCall), "e", expression("e")),
                                        values("array_of_struct", "e"))));
    }

    @Test
    public void testPushDownSubscriptLambdaThroughFilter()
    {
        PushDownSubscriptLambdaThroughFilter pushDownSubscriptLambdaThroughFilter = new PushDownSubscriptLambdaThroughFilter(
                tester().getTypeAnalyzer());

        // Base symbol referenced by other assignments, skip the optimization
        tester().assertThat(pushDownSubscriptLambdaThroughFilter)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall, arrayOfRow, new SymbolReference("array_of_struct")),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.project(
                                            Assignments.identity(arrayOfRow, e),
                                            p.values(arrayOfRow, e))));
                }).doesNotFire();

        // No filter node, skip the rule
        tester().assertThat(pushDownSubscriptLambdaThroughFilter)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall),
                            p.project(
                                    Assignments.identity(arrayOfRow),
                                    p.values(arrayOfRow)));
                }).doesNotFire();

        // Base symbol referenced by predicate in filter node, skip the optimization
        tester().assertThat(pushDownSubscriptLambdaThroughFilter)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall),
                            p.filter(PlanBuilder.expression("array_of_struct <> null"),
                                    p.project(
                                            Assignments.identity(arrayOfRow),
                                            p.values(arrayOfRow))));
                }).doesNotFire();

        // Subscript Lambda being pushed down to the lower projection through filter, with other symbols kept in projections
        tester().assertThat(pushDownSubscriptLambdaThroughFilter)
                .on(p -> {
                    Symbol arrayOfRow = p.symbol("array_of_struct", ARRAY_ROW_TYPE);
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall, e, new SymbolReference("e")),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.project(
                                            Assignments.identity(arrayOfRow, e),
                                            p.values(arrayOfRow, e))));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression("expr"), "e", expression("e")),
                                filter("e > 0",
                                        project(
                                                ImmutableMap.of("expr", expression(subscriptfunctionCall), "e", expression("e")),
                                                project(
                                                        ImmutableMap.of("array_of_struct", expression("array_of_struct"), "e", expression("e")),
                                                        values("array_of_struct", "e"))))));
    }
}
