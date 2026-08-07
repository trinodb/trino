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
package io.trino.spi.function;

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConstantSpecializedImplementation
{
    @Test
    public void testRequiresConsumedArgument()
    {
        ScalarFunctionImplementation implementation = ScalarFunctionImplementation.builder()
                .methodHandle(MethodHandles.constant(long.class, 1L))
                .instanceFactory(MethodHandles.constant(Object.class, new Object()))
                .build();

        assertThatThrownBy(() -> new ConstantSpecializedImplementation(implementation, Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("consumedArguments is empty");
    }

    @Test
    public void testRequiresInstanceFactory()
    {
        ScalarFunctionImplementation implementation = ScalarFunctionImplementation.builder()
                .methodHandle(MethodHandles.constant(long.class, 1L))
                .build();

        assertThatThrownBy(() -> new ConstantSpecializedImplementation(implementation, Set.of(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("constant specialized implementation does not have an instance factory");
    }

    @Test
    public void testContextArgumentsAlignWithConvention()
    {
        InvocationConvention convention = InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL);
        assertThatThrownBy(() -> new ScalarFunctionSpecializationContext(convention, List.of(Optional.empty(), Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected 1 constant arguments, but got 2");
    }
}
