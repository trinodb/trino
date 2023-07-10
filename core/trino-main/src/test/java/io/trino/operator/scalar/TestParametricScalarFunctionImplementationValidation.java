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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.Reflection.methodHandle;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestParametricScalarFunctionImplementationValidation
{
    private static final MethodHandle STATE_FACTORY = methodHandle(TestParametricScalarFunctionImplementationValidation.class, "createState");

    @Test
    public void testConnectorSessionPosition()
    {
        // Without cached instance factory
        MethodHandle validFunctionMethodHandle = methodHandle(TestParametricScalarFunctionImplementationValidation.class, "validConnectorSessionParameterPosition", ConnectorSession.class, long.class, long.class);
        ChoicesSpecializedSqlScalarFunction validFunction = new ChoicesSpecializedSqlScalarFunction(
                new BoundSignature("test", BIGINT, ImmutableList.of(BIGINT, BIGINT)),
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                validFunctionMethodHandle);
        assertEquals(validFunction.getChoices().get(0).getMethodHandle(), validFunctionMethodHandle);

        assertThatThrownBy(() -> new ChoicesSpecializedSqlScalarFunction(
                new BoundSignature("test", BIGINT, ImmutableList.of(BIGINT, BIGINT)),
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle(TestParametricScalarFunctionImplementationValidation.class, "invalidConnectorSessionParameterPosition", long.class, long.class, ConnectorSession.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ConnectorSession must be the first argument when instanceFactory is not present");

        // With cached instance factory
        MethodHandle validFunctionWithInstanceFactoryMethodHandle = methodHandle(TestParametricScalarFunctionImplementationValidation.class, "validConnectorSessionParameterPosition", Object.class, ConnectorSession.class, long.class, long.class);
        ChoicesSpecializedSqlScalarFunction validFunctionWithInstanceFactory = new ChoicesSpecializedSqlScalarFunction(
                new BoundSignature("test", BIGINT, ImmutableList.of(BIGINT, BIGINT)),
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                validFunctionWithInstanceFactoryMethodHandle,
                Optional.of(STATE_FACTORY));
        assertEquals(validFunctionWithInstanceFactory.getChoices().get(0).getMethodHandle(), validFunctionWithInstanceFactoryMethodHandle);

        assertThatThrownBy(() -> new ChoicesSpecializedSqlScalarFunction(
                new BoundSignature("test", BIGINT, ImmutableList.of(BIGINT, BIGINT)),
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle(TestParametricScalarFunctionImplementationValidation.class, "invalidConnectorSessionParameterPosition", Object.class, long.class, long.class, ConnectorSession.class),
                Optional.of(STATE_FACTORY)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ConnectorSession must be the second argument when instanceFactory is present");
    }

    public static Object createState()
    {
        return null;
    }

    public static long validConnectorSessionParameterPosition(ConnectorSession session, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long validConnectorSessionParameterPosition(Object state, ConnectorSession session, long arg1, long arg2)
    {
        return arg1 + arg2;
    }

    public static long invalidConnectorSessionParameterPosition(long arg1, long arg2, ConnectorSession session)
    {
        return arg1 + arg2;
    }

    public static long invalidConnectorSessionParameterPosition(Object state, long arg1, long arg2, ConnectorSession session)
    {
        return arg1 + arg2;
    }
}
