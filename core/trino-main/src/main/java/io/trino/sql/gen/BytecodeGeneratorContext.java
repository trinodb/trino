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
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.sql.relational.RowExpression;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.gen.BytecodeUtils.generateFullInvocation;
import static io.trino.sql.gen.BytecodeUtils.generateInvocation;
import static java.util.Objects.requireNonNull;

public class BytecodeGeneratorContext
{
    private final RowExpressionCompiler rowExpressionCompiler;
    private final Scope scope;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final FunctionManager functionManager;
    private final Variable wasNull;
    private final ClassDefinition classDefinition;
    private final List<Parameter> contextArguments;  // arguments that need to be propagated to generated methods to be able to resolve underlying references, session, etc.

    public BytecodeGeneratorContext(
            RowExpressionCompiler rowExpressionCompiler,
            Scope scope,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionManager functionManager,
            ClassDefinition classDefinition,
            List<Parameter> contextArguments)
    {
        requireNonNull(rowExpressionCompiler, "rowExpressionCompiler is null");
        requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(functionManager, "functionManager is null");
        requireNonNull(classDefinition, "classDefinition is null");

        this.rowExpressionCompiler = rowExpressionCompiler;
        this.scope = scope;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.functionManager = functionManager;
        this.wasNull = scope.getVariable("wasNull");
        this.classDefinition = classDefinition;
        this.contextArguments = ImmutableList.copyOf(contextArguments);
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

    public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction resolvedFunction, InvocationConvention invocationConvention)
    {
        return functionManager.getScalarFunctionImplementation(resolvedFunction, invocationConvention);
    }

    /**
     * Generates a function call with null handling, automatic binding of session parameter, etc.
     */
    public BytecodeNode generateCall(ResolvedFunction resolvedFunction, List<BytecodeNode> arguments)
    {
        return generateInvocation(scope, resolvedFunction, functionManager, arguments, callSiteBinder);
    }

    public BytecodeNode generateFullCall(ResolvedFunction resolvedFunction, List<RowExpression> arguments)
    {
        List<Function<Optional<Class<?>>, BytecodeNode>> argumentCompilers = arguments.stream()
                .map(this::argumentCompiler)
                .collect(toImmutableList());

        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        return generateFullInvocation(scope, resolvedFunction, functionManager, instance, argumentCompilers, callSiteBinder);
    }

    private Function<Optional<Class<?>>, BytecodeNode> argumentCompiler(RowExpression argument)
    {
        return lambdaInterface -> rowExpressionCompiler.compile(argument, scope, lambdaInterface);
    }

    public Variable wasNull()
    {
        return wasNull;
    }

    public ClassDefinition getClassDefinition()
    {
        return classDefinition;
    }

    public RowExpressionCompiler getRowExpressionCompiler()
    {
        return rowExpressionCompiler;
    }

    public CachedInstanceBinder getCachedInstanceBinder()
    {
        return cachedInstanceBinder;
    }

    public FunctionManager getFunctionManager()
    {
        return functionManager;
    }

    public List<Parameter> getContextArguments()
    {
        return contextArguments;
    }
}
