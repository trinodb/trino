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
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;

public class TestMergeProjectWithValues
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNonRowType()
    {
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.valuesOfExpressions(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of(new Cast(
                                                new Row(ImmutableList.of(new NullLiteral(), new NullLiteral())),
                                                toSqlType(RowType.anonymous(ImmutableList.of(BIGINT, BIGINT))))))))
                .doesNotFire();
    }

    @Test
    public void testProjectWithoutOutputSymbols()
    {
        // ValuesNode has two output symbols and two rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.valuesOfExpressions(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of(
                                                new Row(ImmutableList.of(new CharLiteral("x"), new BooleanLiteral("true"))),
                                                new Row(ImmutableList.of(new CharLiteral("y"), new BooleanLiteral("false")))))))
                .matches(values(2));

        // ValuesNode has no output symbols and two rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of()))))
                .matches(values(2));

        // ValuesNode has two output symbols and no rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of())))
                .matches(values());

        // ValuesNode has no output symbols and no rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of())))
                .matches(values());
    }

    @Test
    public void testValuesWithoutOutputSymbols()
    {
        // ValuesNode has two rows. Projected expressions are reproduced for every row of ValuesNode.
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("a"), new CharLiteral("x"), p.symbol("b"), new BooleanLiteral("true")),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of()))))
                .matches(values(
                        ImmutableList.of("a", "b"),
                        ImmutableList.of(
                                ImmutableList.of(new CharLiteral("x"), new BooleanLiteral("true")),
                                ImmutableList.of(new CharLiteral("x"), new BooleanLiteral("true")))));

        // ValuesNode has no rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("a"), new CharLiteral("x"), p.symbol("b"), new BooleanLiteral("true")),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of())))
                .matches(values(ImmutableList.of("a", "b"), ImmutableList.of()));
    }

    @Test
    public void testNonDeterministicValues()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("random"), ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("rand"), expression("rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand")),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .matches(
                        values(
                                ImmutableList.of("rand"),
                                ImmutableList.of(ImmutableList.of(randomFunction))));

        // ValuesNode has multiple rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("output"), expression("value")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("value")),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(new NullLiteral())),
                                        new Row(ImmutableList.of(randomFunction)),
                                        new Row(ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction)))))))
                .matches(
                        values(
                                ImmutableList.of("output"),
                                ImmutableList.of(
                                        ImmutableList.of(new NullLiteral()),
                                        ImmutableList.of(randomFunction),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction)))));

        // ValuesNode has multiple non-deterministic outputs
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x"), expression("-a"),
                                p.symbol("y"), expression("b")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(new DoubleLiteral("1e0"), randomFunction)),
                                        new Row(ImmutableList.of(randomFunction, new NullLiteral())),
                                        new Row(ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction), new NullLiteral()))))))
                .matches(
                        values(
                                ImmutableList.of("x", "y"),
                                ImmutableList.of(
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, new DoubleLiteral("1e0")), randomFunction),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction), new NullLiteral()),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, new ArithmeticUnaryExpression(MINUS, randomFunction)), new NullLiteral()))));
    }

    @Test
    public void testDoNotFireOnNonDeterministicValues()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("random"), ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x"), expression("rand"),
                                p.symbol("y"), expression("rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand")),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .doesNotFire();

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), expression("rand + rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand")),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .doesNotFire();
    }

    @Test
    public void testCorrelation()
    {
        // correlation symbol in projection (note: the resulting plan is not yet supported in execution)
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), expression("a + corr")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new ArithmeticBinaryExpression(ADD, new LongLiteral("1"), new SymbolReference("corr"))))));

        // correlation symbol in values (note: the resulting plan is not yet supported in execution)
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), expression("a")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new SymbolReference("corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new SymbolReference("corr")))));

        // correlation symbol is not present in the resulting expression
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), expression("1")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new SymbolReference("corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new LongLiteral("1")))));
    }

    @Test
    public void testFailingExpression()
    {
        FunctionCall failFunction = failFunction(tester().getMetadata(), tester().getSession(), GENERIC_USER_ERROR, "message");

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), failFunction),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(failFunction))));
    }

    @Test
    public void testMergeProjectWithValues()
    {
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    Symbol f = p.symbol("f");
                    Assignments.Builder assignments = Assignments.builder();
                    assignments.putIdentity(a); // identity assignment
                    assignments.put(d, b.toSymbolReference()); // renaming assignment
                    assignments.put(e, new IsNullPredicate(a.toSymbolReference())); // expression involving input symbol
                    assignments.put(f, new LongLiteral("1")); // constant expression
                    return p.project(
                            assignments.build(),
                            p.valuesOfExpressions(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of(
                                            new Row(ImmutableList.of(new CharLiteral("x"), new BooleanLiteral("true"), new LongLiteral("1"))),
                                            new Row(ImmutableList.of(new CharLiteral("y"), new BooleanLiteral("false"), new LongLiteral("2"))),
                                            new Row(ImmutableList.of(new CharLiteral("z"), new BooleanLiteral("true"), new LongLiteral("3"))))));
                })
                .matches(values(
                        ImmutableList.of("a", "d", "e", "f"),
                        ImmutableList.of(
                                ImmutableList.of(new CharLiteral("x"), new BooleanLiteral("true"), new IsNullPredicate(new CharLiteral("x")), new LongLiteral("1")),
                                ImmutableList.of(new CharLiteral("y"), new BooleanLiteral("false"), new IsNullPredicate(new CharLiteral("y")), new LongLiteral("1")),
                                ImmutableList.of(new CharLiteral("z"), new BooleanLiteral("true"), new IsNullPredicate(new CharLiteral("z")), new LongLiteral("1")))));

        // ValuesNode has no rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    Symbol f = p.symbol("f");
                    Assignments.Builder assignments = Assignments.builder();
                    assignments.putIdentity(a); // identity assignment
                    assignments.put(d, b.toSymbolReference()); // renaming assignment
                    assignments.put(e, new IsNullPredicate(a.toSymbolReference())); // expression involving input symbol
                    assignments.put(f, new LongLiteral("1")); // constant expression
                    return p.project(
                            assignments.build(),
                            p.values(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of()));
                })
                .matches(values(ImmutableList.of("a", "d", "e", "f"), ImmutableList.of()));
    }
}
