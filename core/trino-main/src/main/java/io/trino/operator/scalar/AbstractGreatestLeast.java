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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;
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
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.MinMaxCompare.getMinMaxCompare;
import static io.trino.util.MinMaxCompare.getMinMaxCompareFunctionDependencies;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

public abstract class AbstractGreatestLeast
        extends SqlScalarFunction
{
    private final boolean min;

    protected AbstractGreatestLeast(boolean min, String description)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(min ? "least" : "greatest")
                        .orderableTypeParameter("E")
                        .returnType(new TypeSignature("E"))
                        .argumentType(new TypeSignature("E"))
                        .variableArity()
                        .build())
                .nullable()
                .argumentNullability(true)
                .description(description)
                .build());
        this.min = min;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return getMinMaxCompareFunctionDependencies(new TypeSignature("E"), min);
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type type = boundSignature.getReturnType();
        checkArgument(type.isOrderable(), "Type must be orderable");

        MethodHandle compareMethod = getMinMaxCompare(functionDependencies, type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL), min);

        List<Class<?>> javaTypes = IntStream.range(0, boundSignature.getArity())
                .mapToObj(i -> wrap(type.getJavaType()))
                .collect(toImmutableList());

        Class<?> clazz = generate(javaTypes, compareMethod);
        MethodHandle methodHandle = methodHandle(clazz, getFunctionMetadata().getSignature().getName(), javaTypes.toArray(new Class<?>[0]));

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
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
}
