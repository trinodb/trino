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
import io.trino.spi.connector.ConnectorExpressionEvaluator.EvaluationResult;
import io.trino.spi.connector.ConnectorExpressionEvaluator.Prepared;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SecureExpression;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.EngineExpressions.buildEngineExpression;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInternalConnectorExpressionEvaluator
{
    private static final Symbol PARTITION = new Symbol(BIGINT, "partition");
    private static final Symbol OTHER = new Symbol(BIGINT, "other");
    private static final Expression PARTITION_PREDICATE = comparison(EQUAL, PARTITION.toSymbolReference(), new Constant(BIGINT, 7L));
    private static final Expression OTHER_PREDICATE = comparison(EQUAL, OTHER.toSymbolReference(), new Constant(BIGINT, 11L));

    @Test
    public void testPartitionBindings()
    {
        for (Expression predicate : List.of(PARTITION_PREDICATE, new SecureExpression(PARTITION_PREDICATE))) {
            Prepared prepared = prepare(predicate);
            assertThat(prepared.getArguments()).containsExactly("partition");
            assertThat(prepared.tryEvaluate(Map.of())).isEqualTo(new EvaluationResult.NoResult());
            assertThat(prepared.tryEvaluate(bindings(OTHER, 8L))).isEqualTo(new EvaluationResult.NoResult());
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 7L))).isEqualTo(new EvaluationResult.Value(true));
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 8L))).isEqualTo(new EvaluationResult.Value(false));
            assertThat(prepared.tryEvaluate(bindings(PARTITION, null))).isEqualTo(new EvaluationResult.Value(null));
        }
    }

    @Test
    public void testConstantPredicates()
    {
        for (Constant constant : List.of(TRUE, FALSE, NULL_BOOLEAN)) {
            for (Expression predicate : List.of(constant, new SecureExpression(constant))) {
                assertThat(prepare(predicate).tryEvaluate(Map.of())).isEqualTo(new EvaluationResult.Value(constant.value()));
            }
        }
        assertThat(prepare(new SecureExpression(new SecureExpression(FALSE))).tryEvaluate(Map.of())).isEqualTo(new EvaluationResult.Value(false));
    }

    @Test
    public void testPartiallyBoundConjunctions()
    {
        for (Expression predicate : List.of(
                Logical.and(new SecureExpression(PARTITION_PREDICATE), OTHER_PREDICATE),
                new SecureExpression(Logical.and(PARTITION_PREDICATE, OTHER_PREDICATE)))) {
            Prepared prepared = prepare(predicate);
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 7L))).isEqualTo(new EvaluationResult.NoResult());
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 8L))).isEqualTo(new EvaluationResult.Value(false));
            assertThat(prepared.tryEvaluate(bindings(OTHER, 12L))).isEqualTo(new EvaluationResult.Value(false));
            assertThat(prepared.tryEvaluate(bothBindings(7L, 11L))).isEqualTo(new EvaluationResult.Value(true));
            assertThat(prepared.tryEvaluate(bothBindings(null, 11L))).isEqualTo(new EvaluationResult.Value(null));
        }
    }

    @Test
    public void testPartiallyBoundDisjunctions()
    {
        for (Expression predicate : List.of(
                Logical.or(new SecureExpression(PARTITION_PREDICATE), OTHER_PREDICATE),
                new SecureExpression(Logical.or(PARTITION_PREDICATE, OTHER_PREDICATE)))) {
            Prepared prepared = prepare(predicate);
            // An unbound disjunct may still match
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 8L))).isEqualTo(new EvaluationResult.NoResult());
            assertThat(prepared.tryEvaluate(bindings(PARTITION, 7L))).isEqualTo(new EvaluationResult.Value(true));
            assertThat(prepared.tryEvaluate(bothBindings(7L, 12L))).isEqualTo(new EvaluationResult.Value(true));
            assertThat(prepared.tryEvaluate(bothBindings(8L, 11L))).isEqualTo(new EvaluationResult.Value(true));
            assertThat(prepared.tryEvaluate(bothBindings(8L, 12L))).isEqualTo(new EvaluationResult.Value(false));
            assertThat(prepared.tryEvaluate(bothBindings(null, 12L))).isEqualTo(new EvaluationResult.Value(null));
        }
    }

    @Test
    public void testNestedLogicalExpressions()
    {
        Prepared prepared = prepare(Logical.and(
                new SecureExpression(PARTITION_PREDICATE),
                Logical.or(new SecureExpression(OTHER_PREDICATE), new SecureExpression(NULL_BOOLEAN))));
        assertThat(prepared.tryEvaluate(bindings(PARTITION, 7L))).isEqualTo(new EvaluationResult.NoResult());
        assertThat(prepared.tryEvaluate(bindings(OTHER, 12L))).isEqualTo(new EvaluationResult.NoResult());
        assertThat(prepared.tryEvaluate(bothBindings(7L, 11L))).isEqualTo(new EvaluationResult.Value(true));
        assertThat(prepared.tryEvaluate(bothBindings(7L, 12L))).isEqualTo(new EvaluationResult.Value(null));
        assertThat(prepared.tryEvaluate(bothBindings(8L, 12L))).isEqualTo(new EvaluationResult.Value(false));
    }

    @Test
    public void testRecoverableEvaluationFailure()
    {
        Expression predicate = comparison(
                EQUAL,
                new Call(
                        new TestingFunctionResolution().resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT)),
                        List.of(new Constant(BIGINT, 1L), PARTITION.toSymbolReference())),
                new Constant(BIGINT, 1L));
        for (Expression expression : List.of(predicate, new SecureExpression(predicate))) {
            assertThat(prepare(expression).tryEvaluate(bindings(PARTITION, 0L))).isEqualTo(new EvaluationResult.NoResult());
            assertThat(prepare(expression).tryEvaluate(bindings(PARTITION, 1L))).isEqualTo(new EvaluationResult.Value(true));
        }
    }

    private static Prepared prepare(Expression predicate)
    {
        return new InternalConnectorExpressionEvaluator(PLANNER_CONTEXT)
                .prepare(TEST_SESSION.toConnectorSession(), buildEngineExpression(predicate, PLANNER_CONTEXT.getExpressionCodec()));
    }

    private static Map<String, NullableValue> bindings(Symbol symbol, Long value)
    {
        return Map.of(symbol.name(), new NullableValue(BIGINT, value));
    }

    private static Map<String, NullableValue> bothBindings(Long partition, Long other)
    {
        return Map.of(
                PARTITION.name(), new NullableValue(BIGINT, partition),
                OTHER.name(), new NullableValue(BIGINT, other));
    }
}
