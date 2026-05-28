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
import io.airlift.slice.Slices;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.InstanceMethod;
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

import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMethodCall
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

    @ScalarFunction("char_length")
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long varcharCharLength(@SqlType(StandardTypes.VARCHAR) Slice self)
    {
        return self.toStringUtf8().length();
    }

    @ScalarFunction("repeat")
    @InstanceMethod
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharRepeat(@SqlType(StandardTypes.VARCHAR) Slice self, @SqlType(StandardTypes.BIGINT) long count)
    {
        return Slices.utf8Slice(self.toStringUtf8().repeat((int) count));
    }

    @ScalarFunction("from_string")
    @StaticMethod(StandardTypes.BIGINT)
    @SqlType(StandardTypes.BIGINT)
    public static long bigintFromString(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return Long.parseLong(value.toStringUtf8());
    }

    @Test
    public void testReceiverInParens()
    {
        assertThat(assertions.expression("('hello').char_length()"))
                .matches("BIGINT '5'");
    }

    @Test
    public void testReceiverIsFunctionCall()
    {
        assertThat(assertions.expression("upper('ab').char_length()"))
                .matches("BIGINT '2'");
    }

    @Test
    public void testWithArguments()
    {
        assertThat(assertions.expression("('ab').repeat(3)"))
                .matches("VARCHAR 'ababab'");
    }

    @Test
    public void testBareReceiverResolvesAsMethod()
    {
        // SQL:2023 6.3 SR 2: A.B(args) is treated as a method invocation when applicable.
        // Here `s` is a column of type VARCHAR, so s.char_length() resolves to the
        // varchar method rather than a function named "s.char_length".
        assertThat(assertions.query("SELECT s.char_length() FROM (VALUES VARCHAR 'hi') t(s)"))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testUnknownMethod()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("('hello').nope()").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testInstanceMethodNotResolvableAsFunction()
    {
        // The plain `char_length('hello')` form must NOT resolve to the instance method.
        assertTrinoExceptionThrownBy(() -> assertions.expression("char_length('hello')").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testInstanceMethodNotResolvableAsStaticMethod()
    {
        // `char_length` is an @InstanceMethod, so the static-method form must not find it.
        assertTrinoExceptionThrownBy(() -> assertions.expression("varchar::char_length('hello')").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testStaticMethodNotResolvableAsInstanceMethod()
    {
        // `from_string` is a @StaticMethod on bigint, so the instance-method form must not find it.
        assertTrinoExceptionThrownBy(() -> assertions.expression("('42').from_string()").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testReceiverCoercion()
    {
        // VARCHAR(2) coerces to the method's unbounded VARCHAR receiver type.
        assertThat(assertions.expression("CAST('hi' AS VARCHAR(2)).char_length()"))
                .matches("BIGINT '2'");
    }

    @Test
    public void testReceiverNotCoercible()
    {
        // INTEGER has no implicit coercion to VARCHAR, so no instance method named `char_length` is found for it.
        assertTrinoExceptionThrownBy(() -> assertions.expression("(42).char_length()").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testArgumentCoercion()
    {
        // TINYINT coerces to BIGINT, matching the declared argument type.
        assertThat(assertions.expression("('ab').repeat(TINYINT '3')"))
                .matches("VARCHAR 'ababab'");
    }

    @Test
    public void testArgumentNotCoercible()
    {
        // VARCHAR has no implicit coercion to BIGINT, so the call fails to resolve.
        assertTrinoExceptionThrownBy(() -> assertions.expression("('ab').repeat('three')").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testCaseInsensitiveMethodName()
    {
        assertThat(assertions.expression("('hello').CHAR_LENGTH()"))
                .matches("BIGINT '5'");
        assertThat(assertions.expression("('hello').Char_Length()"))
                .matches("BIGINT '5'");
    }

    @Test
    public void testInstanceMethodRequiresScalarFunctionAnnotation()
    {
        assertTrinoExceptionThrownBy(() -> InternalFunctionBundle.builder().scalars(MissingScalarFunctionFixture.class).build())
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessageContaining("missing @ScalarFunction or @ScalarOperator");
    }

    @Test
    public void testInstanceMethodRequiresSelfArgument()
    {
        assertTrinoExceptionThrownBy(() -> InternalFunctionBundle.builder().scalars(MissingSelfFixture.class).build())
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessageContaining("Instance method nothing must declare a self argument");
    }

    public static class MissingScalarFunctionFixture
    {
        @InstanceMethod
        @SqlType(StandardTypes.BIGINT)
        public static long length(@SqlType(StandardTypes.VARCHAR) Slice self)
        {
            return self.toStringUtf8().length();
        }
    }

    public static class MissingSelfFixture
    {
        @ScalarFunction("nothing")
        @InstanceMethod
        @SqlType(StandardTypes.BIGINT)
        public static long noSelf()
        {
            return 0;
        }
    }
}
