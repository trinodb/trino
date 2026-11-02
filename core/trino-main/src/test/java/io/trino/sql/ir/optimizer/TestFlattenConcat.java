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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.FlattenConcat;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFlattenConcat
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final Reference A = new Reference(VARCHAR, "a");
    private static final Reference B = new Reference(VARCHAR, "b");
    private static final Reference C = new Reference(VARCHAR, "c");
    private static final Reference D = new Reference(VARCHAR, "d");

    @Test
    void testArgumentPositions()
    {
        assertThat(optimize(concat(concat(A, B), C))).contains(concat(A, B, C));
        assertThat(optimize(concat(A, concat(B, C)))).contains(concat(A, B, C));
        assertThat(optimize(concat(A, concat(B, C), D))).contains(concat(A, B, C, D));
        assertThat(optimize(concat(concat(A, B), concat(C, D)))).contains(concat(A, B, C, D));
        assertThat(optimize(concat(A, B))).isEmpty();
        assertThat(optimize(A)).isEmpty();
    }

    @Test
    void testOptimizerIntegration()
    {
        Expression expression = concat(concat(concat(A, B), C), concat(D, A));
        assertThat(IrExpressionOptimizer.newOptimizer(PLANNER_CONTEXT)
                .process(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of()))
                .contains(concat(A, B, C, D, A));
        assertThat(IrExpressionOptimizer.newPartialEvaluator(PLANNER_CONTEXT)
                .process(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    void testNullArguments()
    {
        Constant nullValue = new Constant(VARCHAR, null);
        assertThat(optimize(concat(concat(A, nullValue), B))).contains(concat(A, nullValue, B));
        assertThat(optimize(concat(A, concat(nullValue, B)))).contains(concat(A, nullValue, B));
        assertThat(optimize(concat(concat(A, B), nullValue))).contains(concat(A, B, nullValue));
    }

    @Test
    void testArgumentLimit()
    {
        Call atLimit = concat(expressions().add(concat(A, B)).addAll(nCopies(125, C)).build());
        assertThat(optimize(atLimit))
                .contains(concat(expressions().add(A, B).addAll(nCopies(125, C)).build()));

        Call aboveLimit = concat(expressions().add(concat(A, B)).addAll(nCopies(126, C)).build());
        assertThat(optimize(aboveLimit)).isEmpty();

        Call partial = concat(expressions().add(concat(A, B), concat(C, D)).addAll(nCopies(124, A)).build());
        Call expected = concat(expressions().add(A, B, concat(C, D)).addAll(nCopies(124, A)).build());
        assertThat(optimize(partial)).contains(expected);
        assertThat(optimize(expected)).isEmpty();
    }

    @Test
    void testOtherOverloads()
    {
        for (Type type : ImmutableList.of(VARBINARY, new ArrayType(BIGINT))) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(concat(concat(value, value), value))).isEmpty();
        }
        Reference character = new Reference(createCharType(1), "x");
        assertThat(optimize(concat(concat(character, character), character))).isEmpty();

        Call lower = new Call(FUNCTIONS.resolveFunction("lower", fromTypes(VARCHAR)), ImmutableList.of(concat(A, B)));
        assertThat(optimize(lower)).isEmpty();
        assertThat(optimize(concat(lower, C))).isEmpty();
    }

    @Test
    void testInvalidArityIsNotHidden()
    {
        assertThat(optimize(concat(concat(A), B))).isEmpty();
        assertThat(optimize(concat(concat(A, B)))).isEmpty();
    }

    private static ImmutableList.Builder<Expression> expressions()
    {
        return ImmutableList.builder();
    }

    private static Call concat(Expression... arguments)
    {
        return concat(ImmutableList.copyOf(arguments));
    }

    private static Call concat(List<Expression> arguments)
    {
        return new Call(FUNCTIONS.resolveFunction("concat", fromTypes(arguments.stream().map(Expression::type).toList())), arguments);
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new FlattenConcat(PLANNER_CONTEXT).apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}
