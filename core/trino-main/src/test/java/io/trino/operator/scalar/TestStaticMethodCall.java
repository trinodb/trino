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

import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.StaticMethod;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStaticMethodCall
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(getClass())
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction("parse")
    @StaticMethod(StandardTypes.BIGINT)
    @SqlType(StandardTypes.BIGINT)
    public static long bigintParse(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            return Long.parseLong(value.toStringUtf8());
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse '%s' as bigint".formatted(value.toStringUtf8()));
        }
    }

    @ScalarFunction("array_method")
    @StaticMethod(StandardTypes.ARRAY)
    @SqlType(StandardTypes.BIGINT)
    public static long arrayMethod()
    {
        return 1L;
    }

    @ScalarFunction("row_method")
    @StaticMethod(StandardTypes.ROW)
    @SqlType(StandardTypes.BIGINT)
    public static long rowMethod()
    {
        return 2L;
    }

    @ScalarFunction("map_method")
    @StaticMethod(StandardTypes.MAP)
    @SqlType(StandardTypes.BIGINT)
    public static long mapMethod()
    {
        return 3L;
    }

    @ScalarFunction("token")
    @StaticMethod(StandardTypes.ARRAY)
    @SqlType(StandardTypes.BIGINT)
    public static long arrayToken()
    {
        return 10L;
    }

    @ScalarFunction("token")
    @StaticMethod(StandardTypes.ROW)
    @SqlType(StandardTypes.BIGINT)
    public static long rowToken()
    {
        return 20L;
    }

    @ScalarFunction("token")
    @StaticMethod(StandardTypes.MAP)
    @SqlType(StandardTypes.BIGINT)
    public static long mapToken()
    {
        return 30L;
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.expression("bigint::parse('42')"))
                .matches("BIGINT '42'");

        assertThat(assertions.expression("bigint::parse('-1234567890')"))
                .matches("BIGINT '-1234567890'");
    }

    @Test
    public void testInvalidArgument()
    {
        assertTrinoExceptionThrownBy(assertions.expression("bigint::parse('abc')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testUnknownReceiverType()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("not_a_type::parse('42')").evaluate())
                .hasErrorCode(TYPE_NOT_FOUND)
                .hasMessageContaining("Unknown type: not_a_type");
    }

    @Test
    public void testUnknownMethod()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("bigint::nope('42')").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testStaticMethodNotResolvableWithoutReceiver()
    {
        // The plain `parse('42')` form must NOT resolve to bigint::parse.
        assertTrinoExceptionThrownBy(() -> assertions.expression("parse('42')").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testParametricBaseReceiver()
    {
        // Parametric base types (array, row, map) that have no default instantiation
        // must still resolve as static method receivers — only the base name matters.
        assertThat(assertions.expression("array::array_method()")).matches("BIGINT '1'");
        assertThat(assertions.expression("row::row_method()")).matches("BIGINT '2'");
        assertThat(assertions.expression("map::map_method()")).matches("BIGINT '3'");
    }

    @Test
    public void testSameMethodNameOnDifferentReceivers()
    {
        // Three identically-signed static methods registered on distinct receiver
        // types must coexist in the same function bundle and dispatch by receiver.
        assertThat(assertions.expression("array::token()")).matches("BIGINT '10'");
        assertThat(assertions.expression("row::token()")).matches("BIGINT '20'");
        assertThat(assertions.expression("map::token()")).matches("BIGINT '30'");
    }

    @Test
    public void testParametricReceiverTypeRejected()
    {
        assertThatThrownBy(() -> InternalFunctionBundle.builder().scalars(ParametricReceiverFixture.class).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@StaticMethod receiver type must not have parameters");
    }

    public static class ParametricReceiverFixture
    {
        @ScalarFunction("parse")
        @StaticMethod("varchar(5)")
        @SqlType(StandardTypes.BIGINT)
        public static long parse(@SqlType(StandardTypes.VARCHAR) Slice value)
        {
            return 0;
        }
    }
}
