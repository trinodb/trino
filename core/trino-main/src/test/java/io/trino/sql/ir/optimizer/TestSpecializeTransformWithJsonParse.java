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
import io.trino.operator.scalar.JsonPath;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SpecializeTransformWithJsonParse;
import io.trino.sql.planner.Symbol;
import io.trino.type.FunctionType;
import io.trino.type.JsonPathType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpecializeTransformWithJsonParse
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction JSON_STRING_TO_ARRAY = FUNCTIONS.getCoercion(builtinFunctionName(JSON_STRING_TO_ARRAY_NAME), VARCHAR, new ArrayType(VARCHAR));
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(new ArrayType(VARCHAR), new FunctionType(List.of(VARCHAR), VARCHAR)));
    private static final ResolvedFunction JSON_EXTRACT_SCALAR = FUNCTIONS.resolveFunction("json_extract_scalar", fromTypes(VARCHAR, JsonPathType.JSON_PATH));

    @Test
    void testArray()
    {
        JsonPath jsonPath = new JsonPath("$");
        assertThat(optimize(
                new Call(TRANSFORM,
                        ImmutableList.of(
                                new Call(JSON_STRING_TO_ARRAY, ImmutableList.of(new Reference(VARCHAR, "json_string"))),
                                new Lambda(
                                        ImmutableList.of(new Symbol(VARCHAR, "json_array")),
                                        new Call(JSON_EXTRACT_SCALAR, ImmutableList.of(
                                                new Reference(VARCHAR, "json_array"),
                                                new Constant(JsonPathType.JSON_PATH, jsonPath))))))))
                .isEqualTo(Optional.of(
                        new Call(
                                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction(JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME, fromTypes(VARCHAR, JsonPathType.JSON_PATH)),
                                ImmutableList.of(
                                        new Reference(VARCHAR, "json_string"),
                                        new Constant(JsonPathType.JSON_PATH, jsonPath)))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SpecializeTransformWithJsonParse(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
