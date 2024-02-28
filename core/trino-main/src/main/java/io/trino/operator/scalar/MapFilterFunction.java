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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
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
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.sql.gen.LambdaMetafactoryGenerator.generateMetafactory;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.methodHandle;

public final class MapFilterFunction
        extends SqlScalarFunction
{
    public static final MapFilterFunction MAP_FILTER_FUNCTION = new MapFilterFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapFilterFunction.class, "createState", MapType.class);

    private MapFilterFunction()
    {
        super(FunctionMetadata.scalarBuilder("map_filter")
                .signature(Signature.builder()
                        .typeVariable("K")
                        .typeVariable("V")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(functionType(new TypeSignature("K"), new TypeSignature("V"), BOOLEAN.getTypeSignature()))
                        .build())
                .nondeterministic()
                .description("return map containing entries that match the given predicate")
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType mapType = (MapType) boundSignature.getReturnType();
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, FUNCTION),
                ImmutableList.of(BinaryFunctionInterface.class),
                generateFilter(mapType),
                Optional.of(STATE_FACTORY.bindTo(mapType)));
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return BufferedMapValueBuilder.createBuffered(mapType);
    }

    private static MethodHandle generateFilter(MapType mapType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapFilter"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition filterKeyValue = generateFilterInner(definition, binder, mapType);

        Parameter state = arg("state", Object.class);
        Parameter map = arg("map", SqlMap.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "filter",
                type(SqlMap.class),
                ImmutableList.of(state, map, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Variable mapValueBuilder = scope.declareVariable(BufferedMapValueBuilder.class, "mapValueBuilder");
        body.append(mapValueBuilder.set(state.cast(BufferedMapValueBuilder.class)));

        BytecodeExpression mapEntryBuilder = generateMetafactory(MapValueBuilder.class, filterKeyValue, ImmutableList.of(map, function));
        body.append(mapValueBuilder.invoke("build", SqlMap.class, map.invoke("getSize", int.class), mapEntryBuilder).ret());

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapFilterFunction.class.getClassLoader());
        return methodHandle(generatedClass, "filter", Object.class, SqlMap.class, BinaryFunctionInterface.class);
    }

    private static MethodDefinition generateFilterInner(ClassDefinition definition, CallSiteBinder binder, MapType mapType)
    {
        Parameter map = arg("map", SqlMap.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        Parameter keyBuilder = arg("keyBuilder", BlockBuilder.class);
        Parameter valueBuilder = arg("valueBuilder", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PRIVATE, STATIC),
                "filter",
                type(void.class),
                ImmutableList.of(map, function, keyBuilder, valueBuilder));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();

        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());

        Variable size = scope.declareVariable("size", body, map.invoke("getSize", int.class));
        Variable rawOffset = scope.declareVariable("rawOffset", body, map.invoke("getRawOffset", int.class));
        Variable rawKeyBlock = scope.declareVariable("rawKeyBlock", body, map.invoke("getRawKeyBlock", Block.class));
        Variable rawValueBlock = scope.declareVariable("rawValueBlock", body, map.invoke("getRawValueBlock", Block.class));

        Variable index = scope.declareVariable(int.class, "index");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");
        Variable keep = scope.declareVariable(Boolean.class, "keep");

        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);
        BytecodeNode loadKeyElement;
        if (!keyType.equals(UNKNOWN)) {
            // key element must be non-null
            loadKeyElement = keyElement.set(keySqlType.getValue(rawKeyBlock, add(index, rawOffset)).cast(keyJavaType));
        }
        else {
            loadKeyElement = new BytecodeBlock().append(keyElement.set(constantNull(keyJavaType)));
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
            loadValueElement = new BytecodeBlock().append(valueElement.set(constantNull(valueJavaType)));
        }

        body.append(new ForLoop()
                .initialize(index.set(constantInt(0)))
                .condition(lessThan(index, size))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(keep.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class)).cast(Boolean.class)))
                        .append(new IfStatement("if (keep != null && keep) ...")
                                .condition(and(notEqual(keep, constantNull(Boolean.class)), keep.cast(boolean.class)))
                                .ifTrue(new BytecodeBlock()
                                        .append(keySqlType.invoke("appendTo", void.class, rawKeyBlock, add(index, rawOffset), keyBuilder))
                                        .append(valueSqlType.invoke("appendTo", void.class, rawValueBlock, add(index, rawOffset), valueBuilder))))));
        body.ret();

        return method;
    }
}
