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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.primitives.Primitives.wrap;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.function.OperatorType.COMPARISON;
import static io.prestosql.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

public abstract class AbstractGreatestLeast
        extends SqlScalarFunction
{
    private static final MethodHandle MIN_FUNCTION = methodHandle(AbstractGreatestLeast.class, "min", long.class);
    private static final MethodHandle MAX_FUNCTION = methodHandle(AbstractGreatestLeast.class, "max", long.class);

    private final MethodHandle comparisonResultAdapter;

    protected AbstractGreatestLeast(boolean min, String description)
    {
        super(new FunctionMetadata(
                new Signature(
                        min ? "least" : "greatest",
                        ImmutableList.of(orderableTypeParameter("E")),
                        ImmutableList.of(),
                        new TypeSignature("E"),
                        ImmutableList.of(new TypeSignature("E")),
                        true),
                true,
                ImmutableList.of(new FunctionArgumentDefinition(true)),
                false,
                true,
                description,
                SCALAR));
        this.comparisonResultAdapter = min ? MIN_FUNCTION : MAX_FUNCTION;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(COMPARISON, ImmutableList.of(new TypeSignature("E"), new TypeSignature("E")))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type type = functionBinding.getTypeVariable("E");
        checkArgument(type.isOrderable(), "Type must be orderable");

        MethodHandle compareMethod = functionDependencies.getOperatorInvoker(COMPARISON, ImmutableList.of(type, type), Optional.empty()).getMethodHandle();
        compareMethod = filterReturnValue(compareMethod, comparisonResultAdapter);

        List<Class<?>> javaTypes = IntStream.range(0, functionBinding.getArity())
                .mapToObj(i -> wrap(type.getJavaType()))
                .collect(toImmutableList());

        Class<?> clazz = generate(javaTypes, compareMethod);
        MethodHandle methodHandle = methodHandle(clazz, getFunctionMetadata().getSignature().getName(), javaTypes.toArray(new Class<?>[0]));

        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                NULLABLE_RETURN,
                nCopies(javaTypes.size(), BOXED_NULLABLE),
                methodHandle);
    }

    private Class<?> generate(List<Class<?>> javaTypes, MethodHandle compareMethod)
    {
        Signature signature = getFunctionMetadata().getSignature();
        checkCondition(javaTypes.size() <= 127, NOT_SUPPORTED, "Too many arguments for function call %s()", signature.getName());
        String javaTypeName = javaTypes.stream()
                .map(Class::getSimpleName)
                .collect(joining());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(javaTypeName + "$" + signature.getName()),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        List<Parameter> parameters = IntStream.range(0, javaTypes.size())
                .mapToObj(i -> arg("arg" + i, javaTypes.get(i)))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                signature.getName(),
                type(wrap(javaTypes.get(0))),
                parameters);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        CallSiteBinder binder = new CallSiteBinder();

        Variable value = scope.declareVariable(wrap(javaTypes.get(0)), "value");

        BytecodeExpression nullValue = constantNull(wrap(javaTypes.get(0)));
        body.append(value.set(nullValue));

        LabelNode done = new LabelNode("done");

        compareMethod = compareMethod.asType(methodType(boolean.class, compareMethod.type().wrap().parameterList()));
        for (int i = 0; i < javaTypes.size(); i++) {
            Parameter parameter = parameters.get(i);
            BytecodeExpression invokeCompare = invokeDynamic(
                    BOOTSTRAP_METHOD,
                    ImmutableList.of(binder.bind(compareMethod).getBindingId()),
                    "compare",
                    boolean.class,
                    parameter,
                    value);
            body.append(new IfStatement()
                    .condition(isNull(parameter))
                    .ifTrue(new BytecodeBlock()
                            .append(value.set(nullValue))
                            .gotoLabel(done)));
            body.append(new IfStatement()
                    .condition(or(isNull(value), invokeCompare))
                    .ifTrue(value.set(parameter)));
        }

        body.visitLabel(done);

        body.append(value.ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(getClass().getClassLoader()));
    }

    @UsedByGeneratedCode
    public static boolean min(long comparisonResult)
    {
        return comparisonResult < 0;
    }

    @UsedByGeneratedCode
    public static boolean max(long comparisonResult)
    {
        return comparisonResult > 0;
    }
}
