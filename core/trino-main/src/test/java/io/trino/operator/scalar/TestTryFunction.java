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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTryFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestTryFunction.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction
    @SqlType("bigint")
    public static long throwError()
    {
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "internal error, should not be suppressed by " + TryFunction.NAME);
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.expression("try(42)"))
                .isEqualTo(42);
        assertThat(assertions.expression("try(DOUBLE '4.5')"))
                .isEqualTo(4.5);
        assertThat(assertions.expression("try(DECIMAL '4.5')"))
                .matches("4.5");
        assertThat(assertions.expression("try(TRUE)"))
                .isEqualTo(true);
        assertThat(assertions.expression("try('hello')"))
                .hasType(createVarcharType(5))
                .isEqualTo("hello");
        assertThat(assertions.expression("try(JSON '[true, false, 12, 12.7, \"12\", null]')"))
                .matches("JSON '[true,false,12,12.7,\"12\",null]'");
        assertThat(assertions.expression("try(ARRAY [1, 2])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(1, 2));
        assertThat(assertions.expression("try(TIMESTAMP '2020-05-10 12:34:56.123456789')"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.123456789'");
        assertThat(assertions.expression("try(NULL)"))
                .isNull(UNKNOWN);
    }

    @Test
    public void testExceptions()
    {
        // Exceptions that should be suppressed
        assertThat(assertions.expression("try(1/a)")
                .binding("a", "0"))
                .isNull(INTEGER);
        assertThat(assertions.expression("try(json_parse(a))")
                .binding("a", "'INVALID'"))
                .isNull(JSON);
        assertThat(assertions.expression("try(CAST(a AS INTEGER))")
                .binding("a", "NULL"))
                .isNull(INTEGER);
        assertThat(assertions.expression("try(CAST(a AS TIMESTAMP))")
                .binding("a", "'0000-00-01'"))
                .isNull(TIMESTAMP_MILLIS);
        assertThat(assertions.expression("try(CAST(a AS TIMESTAMP WITH TIME ZONE))")
                .binding("a", "'0000-01-01 ABC'"))
                .isNull(TIMESTAMP_TZ_MILLIS);
        assertThat(assertions.expression("try(abs(a))")
                .binding("a", "-9223372036854775807 - 1"))
                .isNull(BIGINT);

        // JSON path evaluation error (strict path miss)
        assertThat(assertions.expression("try(json_value('[1, 2, 3]', 'strict $[100]' ERROR ON ERROR))"))
                .isNull(VARCHAR);

        // JSON_VALUE result error (path returns multiple items)
        assertThat(assertions.expression("try(json_value('[1, 2, 3]', 'lax $[0 to 2]' ERROR ON ERROR))"))
                .isNull(VARCHAR);

        // JSON input conversion error (malformed input)
        assertThat(assertions.expression("try(json_value('[...', 'lax $[0]' ERROR ON ERROR))"))
                .isNull(VARCHAR);

        // JSON output conversion error (JSON_QUERY with empty path)
        assertThat(assertions.expression("try(json_query('[1, 2, 3]', 'lax $[100]' ERROR ON EMPTY))"))
                .isNull(VARCHAR);

        // Exceptions that should not be suppressed
        assertTrinoExceptionThrownBy(assertions.expression("try(throw_error())")::evaluate)
                .hasErrorCode(GENERIC_INTERNAL_ERROR);
        assertTrinoExceptionThrownBy(assertions.expression("try(fail('boom'))")::evaluate)
                .hasErrorCode(GENERIC_USER_ERROR);
        assertTrinoExceptionThrownBy(assertions.expression("try(json_object('a' : 1, 'a' : 2))")::evaluate)
                .hasErrorCode(NOT_SUPPORTED);
    }
}
