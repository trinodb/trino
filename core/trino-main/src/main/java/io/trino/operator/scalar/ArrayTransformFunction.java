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
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Optional;

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
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.sql.gen.LambdaMetafactoryGenerator.generateMetafactory;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.invoke.MethodHandles.lookup;

public final class ArrayTransformFunction
        extends SqlScalarFunction
{
    private static final MethodHandle CREATE_STATE;

    static {
        try {
            CREATE_STATE = lookup().findStatic(BufferedArrayValueBuilder.class, "createBuffered", MethodType.methodType(BufferedArrayValueBuilder.class, ArrayType.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static final ArrayTransformFunction ARRAY_TRANSFORM_FUNCTION = new ArrayTransformFunction();

    private ArrayTransformFunction()
    {
        super(FunctionMetadata.scalarBuilder("transform")
                .signature(Signature.builder()
                        .typeVariable("T")
                        .typeVariable("U")
                        .returnType(arrayType(new TypeSignature("U")))
                        .argumentType(arrayType(new TypeSignature("T")))
                        .argumentType(functionType(new TypeSignature("T"), new TypeSignature("U")))
                        .build())
                .nondeterministic()
                .description("Apply lambda to each element of the array")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type inputType = ((ArrayType) boundSignature.getArgumentTypes().get(0)).getElementType();
        ArrayType returnType = (ArrayType) boundSignature.getReturnType();
        Type outputType = returnType.getElementType();
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, FUNCTION),
                ImmutableList.of(UnaryFunctionInterface.class),
                generateTransform(inputType, outputType),
                Optional.of(CREATE_STATE.bindTo(returnType)));
    }

    private static MethodHandle generateTransform(Type inputType, Type outputType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("ArrayTransform"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition transformValue = generateTransformValueInner(definition, binder, inputType, outputType);

        Parameter arrayValueBuilder = arg("arrayValueBuilder", BufferedArrayValueBuilder.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", UnaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(arrayValueBuilder, block, function));

        BytecodeExpression arrayBuilder = generateMetafactory(ArrayValueBuilder.class, transformValue, ImmutableList.of(block, function));
        BytecodeExpression entryCount = block.invoke("getPositionCount", int.class);

        method.getBody().append(arrayValueBuilder.invoke("build", Block.class, entryCount, arrayBuilder).ret());

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), ArrayTransformFunction.class.getClassLoader());
        try {
            return lookup().findStatic(generatedClass, "transform", MethodType.methodType(Block.class, BufferedArrayValueBuilder.class, Block.class, UnaryFunctionInterface.class));
        }
        catch (ReflectiveOperationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static MethodDefinition generateTransformValueInner(ClassDefinition definition, CallSiteBinder binder, Type inputType, Type outputType)
    {
        Class<?> inputJavaType = Primitives.wrap(inputType.getJavaType());
        Class<?> outputJavaType = Primitives.wrap(outputType.getJavaType());

        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", UnaryFunctionInterface.class);
        Parameter elementBuilder = arg("elementBuilder", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PRIVATE, STATIC),
                "transformValue",
                type(void.class),
                ImmutableList.of(block, function, elementBuilder));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable inputElement = scope.declareVariable(inputJavaType, "inputElement");
        Variable outputElement = scope.declareVariable(outputJavaType, "outputElement");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        BytecodeNode loadInputElement;
        if (!inputType.equals(UNKNOWN)) {
            loadInputElement = new IfStatement()
                    .condition(block.invoke("isNull", boolean.class, position))
                    .ifTrue(inputElement.set(constantNull(inputJavaType)))
                    .ifFalse(inputElement.set(constantType(binder, inputType).getValue(block, position).cast(inputJavaType)));
        }
        else {
            loadInputElement = new BytecodeBlock().append(inputElement.set(constantNull(inputJavaType)));
        }

        BytecodeNode writeOutputElement;
        if (!outputType.equals(UNKNOWN)) {
            writeOutputElement = new IfStatement()
                    .condition(equal(outputElement, constantNull(outputJavaType)))
                    .ifTrue(elementBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, outputType).writeValue(elementBuilder, outputElement.cast(outputType.getJavaType())));
        }
        else {
            writeOutputElement = new BytecodeBlock().append(elementBuilder.invoke("appendNull", BlockBuilder.class).pop());
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 1))
                .body(new BytecodeBlock()
                        .append(loadInputElement)
                        .append(outputElement.set(function.invoke("apply", Object.class, inputElement.cast(Object.class)).cast(outputJavaType)))
                        .append(writeOutputElement)));

        body.ret();
        return method;
    }
}
