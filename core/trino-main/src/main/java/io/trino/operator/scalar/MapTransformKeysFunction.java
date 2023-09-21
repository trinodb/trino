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
import io.airlift.bytecode.control.TryCatch;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.SqlTypeBytecodeExpression;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;
import io.trino.type.BlockTypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.divide;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.sql.gen.LambdaMetafactoryGenerator.generateMetafactory;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.methodHandle;

public final class MapTransformKeysFunction
        extends SqlScalarFunction
{
    public static final String NAME = "transform_keys";
    private static final MethodHandle STATE_FACTORY = methodHandle(MapTransformKeysFunction.class, "createState", MapType.class);

    public MapTransformKeysFunction(BlockTypeOperators blockTypeOperators)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(NAME)
                        .typeVariable("K1")
                        .typeVariable("K2")
                        .typeVariable("V")
                        .returnType(mapType(new TypeSignature("K2"), new TypeSignature("V")))
                        .argumentType(mapType(new TypeSignature("K1"), new TypeSignature("V")))
                        .argumentType(functionType(new TypeSignature("K1"), new TypeSignature("V"), new TypeSignature("K2")))
                        .build())
                .nondeterministic()
                .description("Apply lambda to each entry of the map and transform the key")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType inputMapType = (MapType) boundSignature.getArgumentType(0);
        Type inputKeyType = inputMapType.getKeyType();
        MapType outputMapType = (MapType) boundSignature.getReturnType();
        Type outputKeyType = outputMapType.getKeyType();
        Type valueType = outputMapType.getValueType();

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, FUNCTION),
                ImmutableList.of(BinaryFunctionInterface.class),
                generateTransformKey(inputKeyType, outputKeyType, valueType),
                Optional.of(STATE_FACTORY.bindTo(outputMapType)));
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return BufferedMapValueBuilder.createBufferedDistinctStrict(mapType);
    }

    private static MethodHandle generateTransformKey(Type keyType, Type transformedKeyType, Type valueType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapTransformKey"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition transformMap = generateTransformKeyInner(definition, binder, keyType, transformedKeyType, valueType);

        Parameter state = arg("state", Object.class);
        Parameter session = arg("session", ConnectorSession.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(state, session, block, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Variable mapValueBuilder = scope.declareVariable(BufferedMapValueBuilder.class, "mapValueBuilder");
        body.append(mapValueBuilder.set(state.cast(BufferedMapValueBuilder.class)));

        BytecodeExpression mapEntryBuilder = generateMetafactory(MapValueBuilder.class, transformMap, ImmutableList.of(session, block, function));
        BytecodeExpression entryCount = divide(block.invoke("getPositionCount", int.class), constantInt(2));

        Variable duplicateKeyException = scope.declareVariable(DuplicateMapKeyException.class, "e");
        body.append(new TryCatch(
                mapValueBuilder.invoke("build", Block.class, entryCount, mapEntryBuilder).ret(),
                ImmutableList.of(
                        new TryCatch.CatchBlock(
                                new BytecodeBlock()
                                        .putVariable(duplicateKeyException)
                                        .append(duplicateKeyException.invoke("withDetailedMessage", DuplicateMapKeyException.class, constantType(binder, transformedKeyType), session))
                                        .throwObject(),
                                ImmutableList.of(type(DuplicateMapKeyException.class))))));

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapTransformKeysFunction.class.getClassLoader());
        return methodHandle(generatedClass, "transform", Object.class, ConnectorSession.class, Block.class, BinaryFunctionInterface.class);
    }

    private static MethodDefinition generateTransformKeyInner(ClassDefinition definition, CallSiteBinder binder, Type keyType, Type transformedKeyType, Type valueType)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        Parameter keyBuilder = arg("keyBuilder", BlockBuilder.class);
        Parameter valueBuilder = arg("valueBuilder", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PRIVATE, STATIC),
                "transform",
                type(void.class),
                ImmutableList.of(session, block, function, keyBuilder, valueBuilder));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> transformedKeyJavaType = Primitives.wrap(transformedKeyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());

        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable transformedKeyElement = scope.declareVariable(transformedKeyJavaType, "transformedKeyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        // throw null key exception block
        BytecodeNode throwNullKeyException = new BytecodeBlock()
                .append(newInstance(
                        TrinoException.class,
                        getStatic(INVALID_FUNCTION_ARGUMENT.getDeclaringClass(), "INVALID_FUNCTION_ARGUMENT").cast(ErrorCodeSupplier.class),
                        constantString("map key cannot be null")))
                .throwObject();

        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);
        BytecodeNode loadKeyElement;
        if (!keyType.equals(UNKNOWN)) {
            loadKeyElement = new BytecodeBlock().append(keyElement.set(keySqlType.getValue(block, position).cast(keyJavaType)));
        }
        else {
            // make sure invokeExact will not take uninitialized keys during compile time but,
            // if we reach this point during runtime, it is an exception
            // also close the block builder before throwing as we may be in a TRY() call
            // so that subsequent calls do not find it in an inconsistent state
            loadKeyElement = new BytecodeBlock()
                    .append(keyElement.set(constantNull(keyJavaType)))
                    .append(throwNullKeyException);
        }

        SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
        BytecodeNode loadValueElement;
        if (!valueType.equals(UNKNOWN)) {
            loadValueElement = new IfStatement()
                    .condition(block.invoke("isNull", boolean.class, add(position, constantInt(1))))
                    .ifTrue(valueElement.set(constantNull(valueJavaType)))
                    .ifFalse(valueElement.set(valueSqlType.getValue(block, add(position, constantInt(1))).cast(valueJavaType)));
        }
        else {
            // make sure invokeExact will not take uninitialized keys during compile time
            loadValueElement = new BytecodeBlock().append(valueElement.set(constantNull(valueJavaType)));
        }

        BytecodeNode writeKeyElement;
        if (!transformedKeyType.equals(UNKNOWN)) {
            writeKeyElement = new BytecodeBlock()
                    .append(transformedKeyElement.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class)).cast(transformedKeyJavaType)))
                    .append(new IfStatement()
                            .condition(equal(transformedKeyElement, constantNull(transformedKeyJavaType)))
                            .ifTrue(throwNullKeyException)
                            .ifFalse(new BytecodeBlock()
                                    .append(constantType(binder, transformedKeyType).writeValue(keyBuilder, transformedKeyElement.cast(transformedKeyType.getJavaType())))
                                    .append(valueSqlType.invoke("appendTo", void.class, block, add(position, constantInt(1)), valueBuilder))));
        }
        else {
            // key cannot be unknown
            // if we reach this point during runtime, it is an exception
            writeKeyElement = throwNullKeyException;
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 2))
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(writeKeyElement)));
        body.ret();
        return method;
    }
}
