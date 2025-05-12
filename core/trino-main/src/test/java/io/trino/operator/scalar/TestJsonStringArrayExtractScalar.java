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

import com.google.common.collect.ImmutableList;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestJsonStringArrayExtractScalar
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    void testEmptyString()
    {
        assertTrinoExceptionThrownBy(() -> extract("", "$").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
    }

    @Test
    void testObject()
    {
        assertTrinoExceptionThrownBy(() -> extract("{}", "$").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
    }

    @Test
    void testScalar()
    {
        assertTrinoExceptionThrownBy(() -> extract("1", "$").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageContaining("Cannot cast to array");
    }

    @Test
    void testEmptyArray()
    {
        assertThat(extract("[]", "$.name"))
                .isEqualTo(ImmutableList.of());
    }

    @Test
    void testScalarArrayExtract()
    {
        assertThat(extract("[1, \"abc\", 2.2]", "$.name"))
                .isEqualTo(asList(null, null, null));
        assertThat(extract("[1, \"abc\", 2.2]", "$"))
                .isEqualTo(asList("1", "abc", "2.2"));
    }

    @Test
    void testEmptyElementsArrayExtract()
    {
        assertThat(extract("[null, [], {}]", "$.name"))
                .isEqualTo(asList(null, null, null));
    }

    @Test
    void testExtract()
    {
        assertThat(extract("[{\"name\": \"a\"}, {\"name\": \"b\"}, [], {}]", "$.name"))
                .isEqualTo(asList("a", "b", null, null));
    }

    @Test
    void testMixedExtract()
    {
        assertThat(extract("[{\"name\": \"a\"}, 1]", "$.name"))
                .isEqualTo(asList("a", null));
    }

    @Test
    void testComplexExtract()
    {
        assertThat(extract("[{\"name\": [1, 2, 3]}, 1]", "$.name"))
                .isEqualTo(asList(null, null));
        assertThat(extract("[{\"name\": {}}, 1]", "$.name"))
                .isEqualTo(asList(null, null));
    }

    private QueryAssertions.ExpressionAssertProvider extract(String json, String jsonPath)
    {
        return assertions.expression("transform(cast(json_parse(json) as array(json)), e -> json_extract_scalar(e, '" + jsonPath + "'))")
                .binding("json", "'" + json + "'");
    }
}
