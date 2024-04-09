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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeterminismEvaluator
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testSanity()
    {
        assertThat(DeterminismEvaluator.isDeterministic(function("rand"))).isFalse();
        assertThat(DeterminismEvaluator.isDeterministic(function("random"))).isFalse();
        assertThat(DeterminismEvaluator.isDeterministic(function("shuffle", ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(new Constant(new ArrayType(VARCHAR), null)))
        )).isFalse();
        assertThat(DeterminismEvaluator.isDeterministic(function("uuid"))).isFalse();
        assertThat(DeterminismEvaluator.isDeterministic(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(new Reference(DOUBLE, "symbol"))))).isTrue();
        assertThat(DeterminismEvaluator.isDeterministic(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(function("rand"))))).isFalse();
        assertThat(DeterminismEvaluator.isDeterministic(
                function(
                        "abs",
                        ImmutableList.of(DOUBLE),
                        ImmutableList.of(function("abs", ImmutableList.of(DOUBLE), ImmutableList.of(new Reference(DOUBLE, "symbol")))))
        )).isTrue();
        assertThat(DeterminismEvaluator.isDeterministic(
                function(
                        "filter",
                        ImmutableList.of(new ArrayType(INTEGER), new FunctionType(ImmutableList.of(INTEGER), BOOLEAN)),
                        ImmutableList.of(new Constant(new ArrayType(INTEGER), null), lambda(new Symbol(INTEGER, "a"), comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)))))
        )).isTrue();
        assertThat(DeterminismEvaluator.isDeterministic(
                function(
                        "filter",
                        ImmutableList.of(new ArrayType(INTEGER), new FunctionType(ImmutableList.of(INTEGER), BOOLEAN)),
                        ImmutableList.of(new Constant(new ArrayType(INTEGER), null), lambda(new Symbol(INTEGER, "a"), comparison(GREATER_THAN, function("rand", ImmutableList.of(INTEGER), ImmutableList.of(new Reference(INTEGER, "a"))), new Constant(INTEGER, 0L)))))
        )).isFalse();
    }

    private Call function(String name)
    {
        return function(name, ImmutableList.of(), ImmutableList.of());
    }

    private Call function(String name, List<Type> types, List<Expression> arguments)
    {
        return functionResolution
                .functionCallBuilder(name)
                .setArguments(types, arguments)
                .build();
    }

    private static Comparison comparison(Comparison.Operator operator, Expression left, Expression right)
    {
        return new Comparison(operator, left, right);
    }

    private static Lambda lambda(Symbol symbol, Expression body)
    {
        return new Lambda(ImmutableList.of(symbol), body);
    }
}
