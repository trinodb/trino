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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Collections.nCopies;

public final class ArrayConstructor
        extends SqlScalarFunction
{
    public static final String NAME = "$array";

    public static final ArrayConstructor ARRAY_CONSTRUCTOR = new ArrayConstructor();

    public ArrayConstructor()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(NAME)
                        .typeVariable("E")
                        .returnType(arrayType(new TypeSignature("E")))
                        .argumentType(new TypeSignature("E"))
                        .argumentType(new TypeSignature("E"))
                        .variableArity()
                        .build())
                .nullable()
                .argumentNullability(true, true)
                .hidden()
                .noDescription()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        Type type = boundSignature.getArgumentTypes().get(0);
        for (int i = 0; i < boundSignature.getArity(); i++) {
            if (type.getJavaType().isPrimitive()) {
                builder.add(Primitives.wrap(type.getJavaType()));
            }
            else {
                builder.add(type.getJavaType());
            }
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateArrayConstructor(stackTypes, type);
        MethodHandle methodHandle = methodHandle(clazz, "arrayConstructor", stackTypes.toArray(new Class<?>[stackTypes.size()]));
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(stackTypes.size(), BOXED_NULLABLE),
                methodHandle);
    }

    private static Class<?> generateArrayConstructor(List<Class<?>> stackTypes, Type elementType)
    {
        checkCondition(stackTypes.size() <= 254, NOT_SUPPORTED, "Too many arguments for array constructor");
        List<String> stackTypeNames = stackTypes.stream()
                .map(Class::getSimpleName)
                .collect(toImmutableList());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("").join(stackTypeNames) + "ArrayConstructor"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate arrayConstructor()
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "arrayConstructor", type(Block.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable blockBuilderVariable = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        CallSiteBinder binder = new CallSiteBinder();

        BytecodeExpression createBlockBuilder = blockBuilderVariable.set(
                constantType(binder, elementType).invoke("createBlockBuilder", BlockBuilder.class, constantNull(BlockBuilderStatus.class), constantInt(stackTypes.size())));
        body.append(createBlockBuilder);

        for (int i = 0; i < stackTypes.size(); i++) {
            Variable argument = scope.getVariable("arg" + i);
            IfStatement ifStatement = new IfStatement()
                    .condition(equal(argument, constantNull(stackTypes.get(i))))
                    .ifTrue(blockBuilderVariable.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, elementType).writeValue(blockBuilderVariable, argument.cast(elementType.getJavaType())));
            body.append(ifStatement);
        }

        body.append(blockBuilderVariable.invoke("build", Block.class).ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(ArrayConstructor.class.getClassLoader()));
    }
}
