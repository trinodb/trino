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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.expression.BytecodeExpression;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;

public final class CallSiteBinder
{
    private int nextId;

    private final Map<Long, MethodHandle> bindings = new HashMap<>();

    public BytecodeExpression loadConstant(Object constant, Class<?> type)
    {
        long binding = bind(MethodHandles.constant(type, constant));
        return invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding),
                "constant_" + binding,
                type);
    }

    public BytecodeExpression invoke(MethodHandle method, String name, BytecodeExpression... parameters)
    {
        return invoke(method, name, ImmutableList.copyOf(parameters));
    }

    public BytecodeExpression invoke(MethodHandle method, String name, List<? extends BytecodeExpression> parameters)
    {
        // ensure that name doesn't have a special characters
        return invokeDynamic(BOOTSTRAP_METHOD, ImmutableList.of(bind(method)), sanitizeName(name), method.type(), parameters);
    }

    private long bind(MethodHandle method)
    {
        long bindingId = nextId++;
        bindings.put(bindingId, method);
        return bindingId;
    }

    public Map<Long, MethodHandle> getBindings()
    {
        return ImmutableMap.copyOf(bindings);
    }

    private static String sanitizeName(String name)
    {
        return name.replaceAll("[^A-Za-z0-9_$]", "_");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }
}
