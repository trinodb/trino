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
package io.prestosql.sql.gen;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.sql.gen.BytecodeUtils.generateInvocation;
import static java.util.Objects.requireNonNull;

public class InvokeFunctionBytecodeExpression
        extends BytecodeExpression
{
    public static BytecodeExpression invokeFunction(Scope scope,
            CachedInstanceBinder cachedInstanceBinder,
            Type type,
            FunctionMetadata functionMetadata,
            Function<InvocationConvention, FunctionInvoker> functionInvokerProvider,
            BytecodeExpression... parameters)
    {
        requireNonNull(scope, "scope is null");
        requireNonNull(functionMetadata, "functionMetadata is null");
        requireNonNull(functionInvokerProvider, "functionInvokerProvider is null");

        return new InvokeFunctionBytecodeExpression(
                scope,
                cachedInstanceBinder.getCallSiteBinder(),
                type,
                functionMetadata,
                functionInvokerProvider,
                ImmutableList.copyOf(parameters));
    }

    private final BytecodeNode invocation;
    private final String oneLineDescription;

    private InvokeFunctionBytecodeExpression(
            Scope scope,
            CallSiteBinder binder,
            Type type,
            FunctionMetadata functionMetadata,
            Function<InvocationConvention, FunctionInvoker> functionInvokerProvider,
            List<BytecodeExpression> parameters)
    {
        super(type(Primitives.unwrap(type.getJavaType())));

        this.invocation = generateInvocation(scope, functionMetadata, functionInvokerProvider, parameters.stream().map(BytecodeNode.class::cast).collect(toImmutableList()), binder);
        this.oneLineDescription = functionMetadata.getSignature().getName() + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return invocation;
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return oneLineDescription;
    }
}
