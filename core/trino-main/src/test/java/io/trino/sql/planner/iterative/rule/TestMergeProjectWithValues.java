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
import io.airlift.slice.Slices;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ArithmeticUnaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

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
                                                new Row(ImmutableList.of(GenericLiteral.constant(UnknownType.UNKNOWN, null), GenericLiteral.constant(UnknownType.UNKNOWN, null))),
                                                RowType.anonymous(ImmutableList.of(BIGINT, BIGINT)))))))
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
                                                new Row(ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), TRUE_LITERAL)),
                                                new Row(ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("y")), FALSE_LITERAL))))))
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
                                Assignments.of(p.symbol("a"), GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), p.symbol("b"), TRUE_LITERAL),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of()))))
                .matches(values(
                        ImmutableList.of("a", "b"),
                        ImmutableList.of(
                                ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), TRUE_LITERAL),
                                ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), TRUE_LITERAL))));

        // ValuesNode has no rows
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("a"), GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), p.symbol("b"), TRUE_LITERAL),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of())))
                .matches(values(ImmutableList.of("a", "b"), ImmutableList.of()));
    }

    @Test
    public void testNonDeterministicValues()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("rand"), new SymbolReference("rand")),
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
                        Assignments.of(p.symbol("output"), new SymbolReference("value")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("value")),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(GenericLiteral.constant(UnknownType.UNKNOWN, null))),
                                        new Row(ImmutableList.of(randomFunction)),
                                        new Row(ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction)))))))
                .matches(
                        values(
                                ImmutableList.of("output"),
                                ImmutableList.of(
                                        ImmutableList.of(GenericLiteral.constant(UnknownType.UNKNOWN, null)),
                                        ImmutableList.of(randomFunction),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction)))));

        // ValuesNode has multiple non-deterministic outputs
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x"), new ArithmeticUnaryExpression(MINUS, new SymbolReference("a")),
                                p.symbol("y"), new SymbolReference("b")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(GenericLiteral.constant(DOUBLE, 1e0), randomFunction)),
                                        new Row(ImmutableList.of(randomFunction, GenericLiteral.constant(UnknownType.UNKNOWN, null))),
                                        new Row(ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction), GenericLiteral.constant(UnknownType.UNKNOWN, null)))))))
                .matches(
                        values(
                                ImmutableList.of("x", "y"),
                                ImmutableList.of(
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, GenericLiteral.constant(DOUBLE, 1e0)), randomFunction),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, randomFunction), GenericLiteral.constant(UnknownType.UNKNOWN, null)),
                                        ImmutableList.of(new ArithmeticUnaryExpression(MINUS, new ArithmeticUnaryExpression(MINUS, randomFunction)), GenericLiteral.constant(UnknownType.UNKNOWN, null)))));
    }

    @Test
    public void testDoNotFireOnNonDeterministicValues()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x"), new SymbolReference("rand"),
                                p.symbol("y"), new SymbolReference("rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand")),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .doesNotFire();

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("rand"), new SymbolReference("rand"))),
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
                        Assignments.of(p.symbol("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("corr"))),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(GenericLiteral.constant(INTEGER, 1L)))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new ArithmeticBinaryExpression(ADD, GenericLiteral.constant(INTEGER, 1L), new SymbolReference("corr"))))));

        // correlation symbol in values (note: the resulting plan is not yet supported in execution)
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), new SymbolReference("a")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new SymbolReference("corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new SymbolReference("corr")))));

        // correlation symbol is not present in the resulting expression
        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), GenericLiteral.constant(INTEGER, 1L)),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new SymbolReference("corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(GenericLiteral.constant(INTEGER, 1L)))));
    }

    @Test
    public void testFailingExpression()
    {
        FunctionCall failFunction = failFunction(tester().getMetadata(), GENERIC_USER_ERROR, "message");

        tester().assertThat(new MergeProjectWithValues(tester().getMetadata()))
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), failFunction),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(GenericLiteral.constant(INTEGER, 1L)))))))
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
                    assignments.put(f, GenericLiteral.constant(INTEGER, 1L)); // constant expression
                    return p.project(
                            assignments.build(),
                            p.valuesOfExpressions(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of(
                                            new Row(ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), TRUE_LITERAL, GenericLiteral.constant(INTEGER, 1L))),
                                            new Row(ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("y")), FALSE_LITERAL, GenericLiteral.constant(INTEGER, 2L))),
                                            new Row(ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("z")), TRUE_LITERAL, GenericLiteral.constant(INTEGER, 3L))))));
                })
                .matches(values(
                        ImmutableList.of("a", "d", "e", "f"),
                        ImmutableList.of(
                                ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x")), TRUE_LITERAL, new IsNullPredicate(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("x"))), GenericLiteral.constant(INTEGER, 1L)),
                                ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("y")), FALSE_LITERAL, new IsNullPredicate(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("y"))), GenericLiteral.constant(INTEGER, 1L)),
                                ImmutableList.of(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("z")), TRUE_LITERAL, new IsNullPredicate(GenericLiteral.constant(createCharType(1), Slices.utf8Slice("z"))), GenericLiteral.constant(INTEGER, 1L)))));

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
                    assignments.put(f, GenericLiteral.constant(INTEGER, 1L)); // constant expression
                    return p.project(
                            assignments.build(),
                            p.values(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of()));
                })
                .matches(values(ImmutableList.of("a", "d", "e", "f"), ImmutableList.of()));
    }
}
