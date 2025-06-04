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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SpecializeCastWithJsonParse;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpecializeCastWithJsonParse
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction JSON_PARSE = FUNCTIONS.resolveFunction("json_parse", fromTypes(VARCHAR));

    @Test
    void testArray()
    {
        assertThat(optimize(
                new Cast(new Call(JSON_PARSE, ImmutableList.of(new Reference(VARCHAR, "x"))), new ArrayType(BIGINT))))
                .isEqualTo(Optional.of(new Call(
                        PLANNER_CONTEXT.getMetadata().getCoercion(builtinFunctionName(JSON_STRING_TO_ARRAY_NAME), VARCHAR, new ArrayType(BIGINT)),
                        ImmutableList.of(new Reference(VARCHAR, "x")))));
    }

    @Test
    void testMap()
    {
        TypeOperators typeOperators = new TypeOperators();

        assertThat(optimize(
                new Cast(new Call(JSON_PARSE, ImmutableList.of(new Reference(VARCHAR, "x"))), new MapType(BIGINT, BIGINT, typeOperators))))
                .isEqualTo(Optional.of(new Call(
                        PLANNER_CONTEXT.getMetadata().getCoercion(builtinFunctionName(JSON_STRING_TO_MAP_NAME), VARCHAR, new MapType(BIGINT, BIGINT, typeOperators)),
                        ImmutableList.of(new Reference(VARCHAR, "x")))));
    }

    @Test
    void testRow()
    {
        assertThat(optimize(
                new Cast(new Call(JSON_PARSE, ImmutableList.of(new Reference(VARCHAR, "x"))), anonymousRow(BIGINT, BIGINT))))
                .isEqualTo(Optional.of(new Call(
                        PLANNER_CONTEXT.getMetadata().getCoercion(builtinFunctionName(JSON_STRING_TO_ROW_NAME), VARCHAR, anonymousRow(BIGINT, BIGINT)),
                        ImmutableList.of(new Reference(VARCHAR, "x")))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SpecializeCastWithJsonParse(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
