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
package io.trino.operator.scalar;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ExpressionAssertProvider;
import io.trino.type.FunctionType;
import io.trino.type.JsonPathType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestJsonStringArrayExtractScalar
{
    private static final String JSON_PATH = "$.property";
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final CatalogSchemaFunctionName TRANSFORM = FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(new ArrayType(JSON), new FunctionType(List.of(JSON), VARCHAR))).name();
    private static final CatalogSchemaFunctionName JSON_STRING_ARRAY_EXTRACT_SCALAR = FUNCTIONS.getMetadata().resolveBuiltinFunction(JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME, fromTypes(VARCHAR, JsonPathType.JSON_PATH)).name();
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    void testEmptySliceExtract()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("transform(cast(json_parse(a) as array<json>), x -> json_extract_scalar(\"x\", '$.name'))")
                .binding("a", "''").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array(varchar). Expected a json array, but got null");
    }

    @Test
    void testEmptyStringExtract()
    {
        failedExtract("", "$");
    }

    @Test
    void testObjectStringExtract()
    {
        failedExtract("{}", "$");
    }

    @Test
    void testScalarStringExtract()
    {
        failedExtract("1", "$");
    }

    @Test
    void testEmptyArrayExtract()
    {
        assertExpression("[]", JSON_PATH, "CAST(ARRAY[] AS ARRAY<VARCHAR>)");
    }

    @Test
    void testScalarArrayExtract()
    {
        String jsonString = "[1, \"abc\", 2.2]";
        assertExpression(jsonString, JSON_PATH, "CAST(ARRAY[null, null, null] AS ARRAY<VARCHAR>)");
        assertExpression(jsonString, "$", "CAST(ARRAY['1', 'abc', '2.2'] as ARRAY<VARCHAR>)");
    }

    @Test
    void testEmptyElementsArrayExtract()
    {
        String jsonString = "[null, [], {}]";
        assertExpression(jsonString, JSON_PATH, "CAST(ARRAY[null, null, null] AS ARRAY<VARCHAR>)");
        assertExpression(jsonString, "$", "CAST(ARRAY[null, null, null] AS ARRAY<VARCHAR>)");
    }

    @Test
    void testExtract()
    {
        assertExpression("[{\"property\": \"a\"}, {\"property\": \"b\"}, [], {}]", JSON_PATH, "CAST(ARRAY['a', 'b', null, null] AS ARRAY<VARCHAR>)");
    }

    @Test
    void testMixedExtract()
    {
        assertExpression("[{\"property\": \"a\"}, 1]", JSON_PATH, "CAST(ARRAY['a', null] AS ARRAY<VARCHAR>)");
    }

    @Test
    void testComplexExtract()
    {
        assertExpression("[{\"property\": [1, 2, 3]}, 1]", JSON_PATH, "CAST(ARRAY[null, null] AS ARRAY<VARCHAR>)");
        assertExpression("[{\"property\": {}}, 1]", JSON_PATH, "CAST(ARRAY[null, null] AS ARRAY<VARCHAR>)");
    }

    private void assertExpression(String jsonString, String jsonPath, String result)
    {
        transformExpression(jsonString, jsonPath)
                .assertThat()
                .matches(result)
                .assignmentAsCall().isFunction(TRANSFORM);
        jsonStringArrayExtractScalarExpression(jsonString, jsonPath)
                .assertThat()
                .matches(result)
                .assignmentAsCall().isFunction(JSON_STRING_ARRAY_EXTRACT_SCALAR);
    }

    private ExpressionAssertProvider transformExpression(String json, String jsonPath)
    {
        return assertions.expression("transform(cast(json_parse('" + json + "') as array<json>), elem -> json_extract_scalar(\"elem\", '" + jsonPath + "'))");
    }

    private ExpressionAssertProvider jsonStringArrayExtractScalarExpression(String json, String jsonPath)
    {
        return assertions.expression("transform(cast(json_parse(json) as array<json>), elem -> json_extract_scalar(\"elem\", '" + jsonPath + "'))")
                .binding("json", "'" + json + "'");
    }

    private void failedExtract(String json, String jsonPath)
    {
        assertTrinoExceptionThrownBy(() -> transformExpression(json, jsonPath).evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
        assertTrinoExceptionThrownBy(() -> jsonStringArrayExtractScalarExpression(json, jsonPath).evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
    }
}
