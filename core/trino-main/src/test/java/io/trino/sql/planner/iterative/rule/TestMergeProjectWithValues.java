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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestMergeProjectWithValues
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction ADD_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction NEGATION_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(DOUBLE));

    @Test
    public void testDoesNotFireOnNonRowType()
    {
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.valuesOfExpressions(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of(new Cast(
                                                new Row(ImmutableList.of(new Constant(UNKNOWN, null), new Constant(UNKNOWN, null))),
                                                RowType.anonymous(ImmutableList.of(BIGINT, BIGINT)))))))
                .doesNotFire();
    }

    @Test
    public void testProjectWithoutOutputSymbols()
    {
        // ValuesNode has two output symbols and two rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.valuesOfExpressions(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of(
                                                new Row(ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("x")), TRUE)),
                                                new Row(ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("y")), FALSE))))))
                .matches(values(2));

        // ValuesNode has no output symbols and two rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of()))))
                .matches(values(2));

        // ValuesNode has two output symbols and no rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(
                                        ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                        ImmutableList.of())))
                .matches(values());

        // ValuesNode has no output symbols and no rows
        tester().assertThat(new MergeProjectWithValues())
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
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("a", createCharType(1)), new Constant(createCharType(1), Slices.utf8Slice("x")), p.symbol("b", BOOLEAN), TRUE),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of()))))
                .matches(values(
                        ImmutableList.of("a", "b"),
                        ImmutableList.of(
                                ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("x")), TRUE),
                                ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("x")), TRUE))));

        // ValuesNode has no rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("a", createCharType(1)), new Constant(createCharType(1), Slices.utf8Slice("x")), p.symbol("b", BOOLEAN), TRUE),
                                p.values(
                                        ImmutableList.of(),
                                        ImmutableList.of())))
                .matches(values(ImmutableList.of("a", "b"), ImmutableList.of()));
    }

    @Test
    public void testNonDeterministicValues()
    {
        Call randomFunction = new Call(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("rand", DOUBLE), new Reference(DOUBLE, "rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand", DOUBLE)),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .matches(
                        values(
                                ImmutableList.of("rand"),
                                ImmutableList.of(ImmutableList.of(randomFunction))));

        // ValuesNode has multiple rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("output", DOUBLE), new Reference(DOUBLE, "value")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("value", DOUBLE)),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(new Constant(DOUBLE, null))),
                                        new Row(ImmutableList.of(randomFunction)),
                                        new Row(ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(randomFunction))))))))
                .matches(
                        values(
                                ImmutableList.of("output"),
                                ImmutableList.of(
                                        ImmutableList.of(new Constant(DOUBLE, null)),
                                        ImmutableList.of(randomFunction),
                                        ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(randomFunction))))));

        // ValuesNode has multiple non-deterministic outputs
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x", DOUBLE), new Call(NEGATION_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "a"))),
                                p.symbol("y", DOUBLE), new Reference(DOUBLE, "b")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a", DOUBLE), p.symbol("b", DOUBLE)),
                                ImmutableList.of(
                                        new Row(ImmutableList.of(new Constant(DOUBLE, 1e0), randomFunction)),
                                        new Row(ImmutableList.of(randomFunction, new Constant(DOUBLE, null))),
                                        new Row(ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(randomFunction)), new Constant(DOUBLE, null)))))))
                .matches(
                        values(
                                ImmutableList.of("x", "y"),
                                ImmutableList.of(
                                        ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 1e0))), randomFunction),
                                        ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(randomFunction)), new Constant(DOUBLE, null)),
                                        ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(new Call(NEGATION_DOUBLE, ImmutableList.of(randomFunction)))), new Constant(DOUBLE, null)))));
    }

    @Test
    public void testDoNotFireOnNonDeterministicValues()
    {
        Call randomFunction = new Call(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()),
                ImmutableList.of());

        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("x", DOUBLE), new Reference(DOUBLE, "rand"),
                                p.symbol("y", DOUBLE), new Reference(DOUBLE, "rand")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand", DOUBLE)),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .doesNotFire();

        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x", DOUBLE), new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "rand"), new Reference(DOUBLE, "rand")))),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("rand", DOUBLE)),
                                ImmutableList.of(new Row(ImmutableList.of(randomFunction))))))
                .doesNotFire();
    }

    @Test
    public void testCorrelation()
    {
        // correlation symbol in projection (note: the resulting plan is not yet supported in execution)
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "corr")))),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a", INTEGER)),
                                ImmutableList.of(new Row(ImmutableList.of(new Constant(INTEGER, 1L)))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Reference(INTEGER, "corr")))))));

        // correlation symbol in values (note: the resulting plan is not yet supported in execution)
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), new Reference(BIGINT, "a")),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new Reference(BIGINT, "corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new Reference(BIGINT, "corr")))));

        // correlation symbol is not present in the resulting expression
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x", INTEGER), new Constant(INTEGER, 1L)),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new Reference(INTEGER, "corr")))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L)))));
    }

    @Test
    public void testFailingExpression()
    {
        Call failFunction = failFunction(tester().getMetadata(), GENERIC_USER_ERROR, "message");

        tester().assertThat(new MergeProjectWithValues())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x", UNKNOWN), failFunction),
                        p.valuesOfExpressions(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(new Row(ImmutableList.of(new Constant(INTEGER, 1L)))))))
                .matches(values(ImmutableList.of("x"), ImmutableList.of(ImmutableList.of(failFunction))));
    }

    @Test
    public void testMergeProjectWithValues()
    {
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    Symbol b = p.symbol("b", BOOLEAN);
                    Symbol c = p.symbol("c", BOOLEAN);
                    Symbol d = p.symbol("d", BOOLEAN);
                    Symbol e = p.symbol("e", BOOLEAN);
                    Symbol f = p.symbol("f", INTEGER);
                    Assignments.Builder assignments = Assignments.builder();
                    assignments.putIdentity(a); // identity assignment
                    assignments.put(d, b.toSymbolReference()); // renaming assignment
                    assignments.put(e, new IsNull(a.toSymbolReference())); // expression involving input symbol
                    assignments.put(f, new Constant(INTEGER, 1L)); // constant expression
                    return p.project(
                            assignments.build(),
                            p.valuesOfExpressions(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of(
                                            new Row(ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("x")), TRUE, new Constant(INTEGER, 1L))),
                                            new Row(ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("y")), FALSE, new Constant(INTEGER, 2L))),
                                            new Row(ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("z")), TRUE, new Constant(INTEGER, 3L))))));
                })
                .matches(values(
                        ImmutableList.of("a", "d", "e", "f"),
                        ImmutableList.of(
                                ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("x")), TRUE, new IsNull(new Constant(createCharType(1), Slices.utf8Slice("x"))), new Constant(INTEGER, 1L)),
                                ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("y")), FALSE, new IsNull(new Constant(createCharType(1), Slices.utf8Slice("y"))), new Constant(INTEGER, 1L)),
                                ImmutableList.of(new Constant(createCharType(1), Slices.utf8Slice("z")), TRUE, new IsNull(new Constant(createCharType(1), Slices.utf8Slice("z"))), new Constant(INTEGER, 1L)))));

        // ValuesNode has no rows
        tester().assertThat(new MergeProjectWithValues())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    Symbol b = p.symbol("b", BOOLEAN);
                    Symbol c = p.symbol("c", BOOLEAN);
                    Symbol d = p.symbol("d", BOOLEAN);
                    Symbol e = p.symbol("e", BOOLEAN);
                    Symbol f = p.symbol("f", INTEGER);
                    Assignments.Builder assignments = Assignments.builder();
                    assignments.putIdentity(a); // identity assignment
                    assignments.put(d, b.toSymbolReference()); // renaming assignment
                    assignments.put(e, new IsNull(a.toSymbolReference())); // expression involving input symbol
                    assignments.put(f, new Constant(INTEGER, 1L)); // constant expression
                    return p.project(
                            assignments.build(),
                            p.values(
                                    ImmutableList.of(a, b, c),
                                    ImmutableList.of()));
                })
                .matches(values(ImmutableList.of("a", "d", "e", "f"), ImmutableList.of()));
    }
}
