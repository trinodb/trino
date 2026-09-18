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
package io.trino.sql.gen;

import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCallSiteBinder
{
    @Test
    public void testBindingKinds()
    {
        CallSiteBinder binder = new CallSiteBinder();
        assertThat(binder.bind(MethodHandles.identity(long.class)).getKind()).isEqualTo(Binding.Kind.HANDLE);
        assertThat(binder.bind(BIGINT, Type.class).getKind()).isEqualTo(Binding.Kind.CONSTANT);
    }

    @Test
    public void testMethodHandleDeduplication()
    {
        CallSiteBinder binder = new CallSiteBinder();
        MethodHandle handle = MethodHandles.identity(long.class);

        Binding first = binder.bind(handle);
        Binding second = binder.bind(handle);
        assertThat(second).isSameAs(first);
        assertThat(binder.getClassData()).hasSize(1);

        // a different handle instance is a different binding, even if behaviorally equal
        // (MethodHandles.identity returns a cached instance, so wrap to force fresh handles)
        Binding other = binder.bind(MethodHandles.dropArguments(handle, 0, int.class));
        assertThat(other).isNotSameAs(first);
        assertThat(binder.getClassData()).hasSize(2);
    }

    @Test
    public void testConstantDeduplication()
    {
        CallSiteBinder binder = new CallSiteBinder();

        Binding first = binder.bind(BIGINT, Type.class);
        Binding second = binder.bind(BIGINT, Type.class);
        assertThat(second).isSameAs(first);
        assertThat(binder.getClassData()).hasSize(1);

        // the same constant bound as a different type is a different binding
        Binding asObject = binder.bind(BIGINT, Object.class);
        assertThat(asObject).isNotSameAs(first);
        assertThat(binder.getClassData()).hasSize(2);
    }

    @Test
    public void testClassDataInvocationRequiresExplicitArguments()
    {
        CallSiteBinder binder = new CallSiteBinder();
        Binding binding = binder.bind(MethodHandles.identity(long.class));

        // explicit arguments produce a dynamic constant invocation
        assertThat(invoke(binding, "test", constantLong(1))).isNotNull();

        // a call site that pushed its arguments onto the operand stack must load the
        // binding handle below them instead; this must fail at generation time
        assertThatThrownBy(() -> invoke(binding, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be passed explicitly");

        // constants load as dynamic constants regardless
        Binding constant = binder.bind(BIGINT, Type.class);
        assertThat(loadConstant(constant)).isNotNull();
    }

    @Test
    public void testZeroArityHandleLoadsAsConstantInvocation()
    {
        CallSiteBinder binder = new CallSiteBinder();
        Binding binding = binder.bind(MethodHandles.constant(long.class, 42L));
        assertThat(binding.getKind()).isEqualTo(Binding.Kind.HANDLE);
        assertThat(binding.getType()).isEqualTo(methodType(long.class));
        // no arguments to pass, so the implicit form is unambiguous
        assertThat(invoke(binding, "test")).isNotNull();
        assertThat(loadConstant(binding)).isNotNull();
    }
}
