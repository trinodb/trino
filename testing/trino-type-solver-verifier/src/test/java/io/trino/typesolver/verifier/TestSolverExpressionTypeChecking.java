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
package io.trino.typesolver.verifier;

import io.trino.lib.TrinoPreset;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.SolverExpressionTypeChecker;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.weakref.solver.TypeLibrary;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

/// Stage-1 prototype validation: scalar expressions — including lambda-taking calls — typed by the
/// solver via [SolverExpressionTypeChecker], with expected types matching what Trino's analyzer
/// produces for the same expressions.
class TestSolverExpressionTypeChecking
{
    private final SqlParser parser = new SqlParser();
    private final TypeLibrary library = TrinoPreset.library();
    private final SolverExpressionTypeChecker checker = new SolverExpressionTypeChecker(
            library.typeSystem(),
            library.resolver(),
            library::functions,
            new TestingFunctionResolution().getPlannerContext().getTypeManager());

    @Test
    void testGroundExpressions()
    {
        assertType("1 + 2", INTEGER);
        assertType("1 + CAST(2 AS bigint)", BIGINT);
        assertType("1.5e0 + 2", DOUBLE);
        assertType("1 > 2", BOOLEAN);
        assertType("length('abc')", BIGINT);
        // length-preserving overload wins over the unbounded one by coercion-count dominance
        assertType("upper('abc')", createVarcharType(3));
        // the preset models string concat as unbounded varchar (a known divergence from Trino's
        // length-summing concat, tracked by the function-parity harness)
        assertType("concat('ab', 'cde')", VARCHAR);
        assertType("sequence(1, 3)", new ArrayType(BIGINT));
    }

    @Test
    void testLambdaArguments()
    {
        // The lambda's parameter type falls out of unifying the array argument against the scheme;
        // the body is then typed with that parameter in scope, and its type completes the call
        assertType("transform(sequence(1, 3), x -> x + 1)", new ArrayType(BIGINT));
        assertType("transform(sequence(1, 3), x -> x > 2)", new ArrayType(BOOLEAN));
        assertType("filter(sequence(1, 3), x -> x > 2)", new ArrayType(BIGINT));
        assertType("any_match(sequence(1, 3), x -> x > 2)", BOOLEAN);
    }

    @Test
    void testMultiParameterLambda()
    {
        // Both parameter types come from different arguments of the same call
        assertType("zip_with(sequence(1, 2), sequence(3, 4), (a, b) -> a + b)", new ArrayType(BIGINT));
        assertType("zip_with(sequence(1, 2), sequence(3, 4), (a, b) -> a > b)", new ArrayType(BOOLEAN));
    }

    @Test
    void testMultipleLambdas()
    {
        // Two lambdas in one call: the state type comes from the initial-value argument, the input
        // type from the array — one probe pass grounds both lambdas' parameter types, and the
        // output lambda's body type alone determines the result
        assertType("reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s * 2.5e0)", DOUBLE);
        assertType("reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s > 5)", BOOLEAN);
    }

    @Test
    void testNestedLambdas()
    {
        // The inner call resolves first (its own probe/discharge cycle), and its result type feeds
        // the outer call's probe
        assertType("transform(transform(sequence(1, 3), x -> x + 1), y -> y * 1.5e0)", new ArrayType(DOUBLE));
        assertType("filter(transform(sequence(1, 3), x -> x + 1), y -> y > 2)", new ArrayType(BIGINT));
    }

    private void assertType(String expression, Type expected)
    {
        assertThat(checker.typeOf(parser.createExpression(expression)))
                .as(expression)
                .isEqualTo(expected);
    }
}
