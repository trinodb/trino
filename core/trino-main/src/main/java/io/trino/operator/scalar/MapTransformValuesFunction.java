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

import com.google.common.base.Throwables;
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
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.SqlTypeBytecodeExpression;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;

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
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
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

public final class MapTransformValuesFunction
        extends SqlScalarFunction
{
    public static final MapTransformValuesFunction MAP_TRANSFORM_VALUES_FUNCTION = new MapTransformValuesFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapTransformKeysFunction.class, "createState", MapType.class);

    private MapTransformValuesFunction()
    {
        super(FunctionMetadata.scalarBuilder("transform_values")
                .signature(Signature.builder()
                        .typeVariable("K")
                        .typeVariable("V1")
                        .typeVariable("V2")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V2")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V1")))
                        .argumentType(functionType(new TypeSignature("K"), new TypeSignature("V1"), new TypeSignature("V2")))
                        .build())
                .nondeterministic()
                .description("Apply lambda to each entry of the map and transform the value")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType inputMapType = (MapType) boundSignature.getArgumentType(0);
        Type inputValueType = inputMapType.getValueType();
        MapType outputMapType = (MapType) boundSignature.getReturnType();
        Type keyType = outputMapType.getKeyType();
        Type outputValueType = outputMapType.getValueType();

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, FUNCTION),
                ImmutableList.of(BinaryFunctionInterface.class),
                generateTransform(keyType, inputValueType, outputValueType),
                Optional.of(STATE_FACTORY.bindTo(outputMapType)));
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return BufferedMapValueBuilder.createBuffered(mapType);
    }

    private static MethodHandle generateTransform(Type keyType, Type valueType, Type transformedValueType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapTransformValue"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition transformMap = generateTransformInner(definition, binder, keyType, valueType, transformedValueType);

        // define transform method
        Parameter state = arg("state", Object.class);
        Parameter map = arg("map", SqlMap.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(SqlMap.class),
                ImmutableList.of(state, map, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Variable mapValueBuilder = scope.declareVariable(BufferedMapValueBuilder.class, "mapValueBuilder");
        body.append(mapValueBuilder.set(state.cast(BufferedMapValueBuilder.class)));

        BytecodeExpression mapEntryBuilder = generateMetafactory(MapValueBuilder.class, transformMap, ImmutableList.of(map, function));
        body.append(mapValueBuilder.invoke("build", SqlMap.class, map.invoke("getSize", int.class), mapEntryBuilder).ret());

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapTransformValuesFunction.class.getClassLoader());
        return methodHandle(generatedClass, "transform", Object.class, SqlMap.class, BinaryFunctionInterface.class);
    }

    private static MethodDefinition generateTransformInner(ClassDefinition definition, CallSiteBinder binder, Type keyType, Type valueType, Type transformedValueType)
    {
        Parameter map = arg("map", SqlMap.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        Parameter keyBuilder = arg("keyBuilder", BlockBuilder.class);
        Parameter valueBuilder = arg("valueBuilder", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PRIVATE, STATIC),
                "transform",
                type(void.class),
                ImmutableList.of(map, function, keyBuilder, valueBuilder));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());
        Class<?> transformedValueJavaType = Primitives.wrap(transformedValueType.getJavaType());

        Variable size = scope.declareVariable("size", body, map.invoke("getSize", int.class));
        Variable rawOffset = scope.declareVariable("rawOffset", body, map.invoke("getRawOffset", int.class));
        Variable rawKeyBlock = scope.declareVariable("rawKeyBlock", body, map.invoke("getRawKeyBlock", Block.class));
        Variable rawValueBlock = scope.declareVariable("rawValueBlock", body, map.invoke("getRawValueBlock", Block.class));

        Variable index = scope.declareVariable(int.class, "index");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");
        Variable transformedValueElement = scope.declareVariable(transformedValueJavaType, "transformedValueElement");

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
            loadKeyElement = keyElement.set(keySqlType.getValue(rawKeyBlock, add(index, rawOffset)).cast(keyJavaType));
        }
        else {
            // make sure invokeExact will not take uninitialized keys during compile time,
            // but if we reach this point during runtime, it is an exception
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
                    .condition(rawValueBlock.invoke("isNull", boolean.class, add(index, rawOffset)))
                    .ifTrue(valueElement.set(constantNull(valueJavaType)))
                    .ifFalse(valueElement.set(valueSqlType.getValue(rawValueBlock, add(index, rawOffset)).cast(valueJavaType)));
        }
        else {
            loadValueElement = valueElement.set(constantNull(valueJavaType));
        }

        BytecodeNode writeTransformedValueElement;
        if (!transformedValueType.equals(UNKNOWN)) {
            writeTransformedValueElement = new IfStatement()
                    .condition(equal(transformedValueElement, constantNull(transformedValueJavaType)))
                    .ifTrue(valueBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, transformedValueType).writeValue(valueBuilder, transformedValueElement.cast(transformedValueType.getJavaType())));
        }
        else {
            writeTransformedValueElement = valueBuilder.invoke("appendNull", BlockBuilder.class).pop();
        }

        Variable transformationException = scope.declareVariable(Throwable.class, "transformationException");
        body.append(new ForLoop()
                .initialize(index.set(constantInt(0)))
                .condition(lessThan(index, size))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(
                                new TryCatch(
                                        "Close builder before throwing to avoid subsequent calls finding it in an inconsistent state if we are in a TRY() call.",
                                        transformedValueElement.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class))
                                                .cast(transformedValueJavaType)),
                                        ImmutableList.of(
                                                new TryCatch.CatchBlock(
                                                        new BytecodeBlock()
                                                                .putVariable(transformationException)
                                                                .append(invokeStatic(Throwables.class, "throwIfUnchecked", void.class, transformationException))
                                                                .append(newInstance(RuntimeException.class, transformationException))
                                                                .throwObject(),
                                                        ImmutableList.of(type(Throwable.class))))))
                        .append(keySqlType.invoke("appendTo", void.class, rawKeyBlock, add(index, rawOffset), keyBuilder))
                        .append(writeTransformedValueElement)));

        body.ret();
        return method;
    }
}
