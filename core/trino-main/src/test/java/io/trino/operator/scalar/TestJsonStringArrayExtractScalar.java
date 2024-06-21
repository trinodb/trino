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

import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ExpressionAssertProvider.Result;
import io.trino.type.FunctionType;
import io.trino.type.JsonPathType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.List;

import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestJsonStringArrayExtractScalar
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(new ArrayType(JSON), new FunctionType(List.of(JSON), VARCHAR)));
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
        assertThat(extract("[]", "$.name").value())
                .isEqualTo(ImmutableList.of());
    }

    @Test
    void testScalarArrayExtract()
    {
        assertThat(extract("[1, \"abc\", 2.2]", "$.name").value())
                .isEqualTo(asList(null, null, null));
        assertThat(extract("[1, \"abc\", 2.2]", "$").value())
                .isEqualTo(asList("1", "abc", "2.2"));
    }

    @Test
    void testEmptyElementsArrayExtract()
    {
        assertThat(extract("[null, [], {}]", "$.name").value())
                .isEqualTo(asList(null, null, null));
    }

    @Test
    void testExtract()
    {
        assertThat(extract("[{\"name\": \"a\"}, {\"name\": \"b\"}, [], {}]", "$.name").value())
                .isEqualTo(asList("a", "b", null, null));
    }

    @Test
    void testMixedExtract()
    {
        assertThat(extract("[{\"name\": \"a\"}, 1]", "$.name").value())
                .isEqualTo(asList("a", null));
    }

    @Test
    void testComplexExtract()
    {
        assertThat(extract("[{\"name\": [1, 2, 3]}, 1]", "$.name").value())
                .isEqualTo(asList(null, null));
        assertThat(extract("[{\"name\": {}}, 1]", "$.name").value())
                .isEqualTo(asList(null, null));
    }

    private Result extract(String json, String jsonPath)
    {
        Result transformationResult = extractTransformationResult(json, jsonPath);
        Result jsonStringResult = extractJsonStringArrayExtractResult(json, jsonPath);
        assertThat(transformationResult.value()).isEqualTo(jsonStringResult.value());
        return jsonStringResult;
    }

    private Result extractTransformationResult(String json, String jsonPath)
    {
        Result result = assertions.expression("transform(cast(json_parse('" + json + "') as array<json>), elem -> json_extract_scalar(\"elem\", '" + jsonPath + "'))")
                .evaluate();
        assertThat(result.expression().isPresent()).isTrue();
        assertThat(result.expression().get()).isInstanceOf(Call.class);
        assertThat(((Call) result.expression().get()).function()).isEqualTo(TRANSFORM);
        return result;
    }

    private Result extractJsonStringArrayExtractResult(String json, String jsonPath)
    {
        Result result = assertions.expression("transform(cast(json_parse(json) as array<json>), elem -> json_extract_scalar(\"elem\", '" + jsonPath + "'))")
                .binding("json", "'" + json + "'")
                .evaluate();
        assertThat(result.expression().isPresent()).isTrue();
        assertThat(result.expression().get()).isInstanceOf(Call.class);
        assertThat(((Call) result.expression().get()).function().name()).isEqualTo(JSON_STRING_ARRAY_EXTRACT_SCALAR);
        return result;
    }

    private void failedExtract(String json, String jsonPath)
    {
        assertTrinoExceptionThrownBy(() -> extractTransformationResult(json, jsonPath))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
        assertTrinoExceptionThrownBy(() -> extractJsonStringArrayExtractResult(json, jsonPath))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
    }
}
