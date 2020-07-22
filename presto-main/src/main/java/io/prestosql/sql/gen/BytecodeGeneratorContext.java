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

import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.sql.relational.RowExpression;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.gen.BytecodeUtils.generateFullInvocation;
import static io.prestosql.sql.gen.BytecodeUtils.generateInvocation;
import static java.util.Objects.requireNonNull;

public class BytecodeGeneratorContext
{
    private final RowExpressionCompiler rowExpressionCompiler;
    private final Scope scope;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final Metadata metadata;
    private final Variable wasNull;

    public BytecodeGeneratorContext(
            RowExpressionCompiler rowExpressionCompiler,
            Scope scope,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Metadata metadata)
    {
        requireNonNull(rowExpressionCompiler, "bytecodeGenerator is null");
        requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(metadata, "metadata is null");

        this.rowExpressionCompiler = rowExpressionCompiler;
        this.scope = scope;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.metadata = metadata;
        this.wasNull = scope.getVariable("wasNull");
    }

    public Scope getScope()
    {
        return scope;
    }

    public CallSiteBinder getCallSiteBinder()
    {
        return callSiteBinder;
    }

    public BytecodeNode generate(RowExpression expression)
    {
        return rowExpressionCompiler.compile(expression, scope);
    }

    public FunctionInvoker getScalarFunctionInvoker(ResolvedFunction resolvedFunction,
            Optional<InvocationConvention> invocationConvention)
    {
        return metadata.getScalarFunctionInvoker(resolvedFunction, invocationConvention);
    }

    /**
     * Generates a function call with null handling, automatic binding of session parameter, etc.
     */
    public BytecodeNode generateCall(ResolvedFunction resolvedFunction, List<BytecodeNode> arguments)
    {
        return generateInvocation(scope, resolvedFunction, metadata, arguments, callSiteBinder);
    }

    public BytecodeNode generateFullCall(ResolvedFunction resolvedFunction, List<RowExpression> arguments)
    {
        List<Function<Optional<Class<?>>, BytecodeNode>> argumentCompilers = arguments.stream()
                .map(this::argumentCompiler)
                .collect(toImmutableList());

        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        return generateFullInvocation(scope, resolvedFunction, metadata, instance, argumentCompilers, callSiteBinder);
    }

    private Function<Optional<Class<?>>, BytecodeNode> argumentCompiler(RowExpression argument)
    {
        return lambdaInterface -> rowExpressionCompiler.compile(argument, scope, lambdaInterface);
    }

    public Variable wasNull()
    {
        return wasNull;
    }
}
