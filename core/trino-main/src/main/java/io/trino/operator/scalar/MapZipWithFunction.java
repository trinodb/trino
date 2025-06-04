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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.util.Reflection.methodHandle;

public final class MapZipWithFunction
        extends SqlScalarFunction
{
    public static final MapZipWithFunction MAP_ZIP_WITH_FUNCTION = new MapZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapZipWithFunction.class, "mapZipWith", Type.class, Type.class, Type.class, MapType.class, Object.class, SqlMap.class, SqlMap.class, MapZipWithLambda.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(MapZipWithFunction.class, "createState", MapType.class);

    private MapZipWithFunction()
    {
        super(FunctionMetadata.scalarBuilder("map_zip_with")
                .signature(Signature.builder()
                        .typeVariable("K")
                        .typeVariable("V1")
                        .typeVariable("V2")
                        .typeVariable("V3")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V3")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V1")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V2")))
                        .argumentType(functionType(new TypeSignature("K"), new TypeSignature("V1"), new TypeSignature("V2"), new TypeSignature("V3")))
                        .build())
                .description("Merge two maps into a single map by applying the lambda function to the pair of values with the same key")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType outputMapType = (MapType) boundSignature.getReturnType();
        Type keyType = outputMapType.getKeyType();
        Type inputValueType1 = ((MapType) boundSignature.getArgumentType(0)).getValueType();
        Type inputValueType2 = ((MapType) boundSignature.getArgumentType(1)).getValueType();
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL, FUNCTION),
                ImmutableList.of(MapZipWithLambda.class),
                METHOD_HANDLE.bindTo(keyType).bindTo(inputValueType1).bindTo(inputValueType2).bindTo(outputMapType),
                Optional.of(STATE_FACTORY.bindTo(outputMapType)));
    }

    public static Object createState(MapType mapType)
    {
        return BufferedMapValueBuilder.createBuffered(mapType);
    }

    public static SqlMap mapZipWith(
            Type keyType,
            Type leftValueType,
            Type rightValueType,
            MapType outputMapType,
            Object state,
            SqlMap leftMap,
            SqlMap rightMap,
            MapZipWithLambda function)
    {
        Type outputValueType = outputMapType.getValueType();

        int leftSize = leftMap.getSize();
        int leftRawOffset = leftMap.getRawOffset();
        Block leftRawKeyBlock = leftMap.getRawKeyBlock();
        Block leftRawValueBlock = leftMap.getRawValueBlock();

        int rightSize = rightMap.getSize();
        int rightRawOffset = rightMap.getRawOffset();
        Block rightRawKeyBlock = rightMap.getRawKeyBlock();
        Block rightRawValueBlock = rightMap.getRawValueBlock();

        int maxOutputSize = (leftSize + rightSize);
        BufferedMapValueBuilder mapValueBuilder = (BufferedMapValueBuilder) state;
        return mapValueBuilder.build(maxOutputSize, (keyBuilder, valueBuilder) -> {
            // seekKey() can take non-trivial time when key is a complicated value, such as a long VARCHAR or ROW.
            boolean[] keyFound = new boolean[rightSize];
            for (int leftIndex = 0; leftIndex < leftSize; leftIndex++) {
                Object key = readNativeValue(keyType, leftRawKeyBlock, leftRawOffset + leftIndex);
                Object leftValue = readNativeValue(leftValueType, leftRawValueBlock, leftRawOffset + leftIndex);

                int rightIndex = rightMap.seekKey(key);
                Object rightValue = null;
                if (rightIndex != -1) {
                    rightValue = readNativeValue(rightValueType, rightRawValueBlock, rightRawOffset + rightIndex);
                    keyFound[rightIndex] = true;
                }

                Object outputValue = function.apply(key, leftValue, rightValue);

                keyType.appendTo(leftRawKeyBlock, leftRawOffset + leftIndex, keyBuilder);
                writeNativeValue(outputValueType, valueBuilder, outputValue);
            }

            // iterate over keys that only exists in rightMap
            for (int rightIndex = 0; rightIndex < rightSize; rightIndex++) {
                if (!keyFound[rightIndex]) {
                    Object key = readNativeValue(keyType, rightRawKeyBlock, rightRawOffset + rightIndex);
                    Object rightValue = readNativeValue(rightValueType, rightRawValueBlock, rightRawOffset + rightIndex);

                    Object outputValue = function.apply(key, null, rightValue);

                    keyType.appendTo(rightRawKeyBlock, rightRawOffset + rightIndex, keyBuilder);
                    writeNativeValue(outputValueType, valueBuilder, outputValue);
                }
            }
        });
    }

    @FunctionalInterface
    public interface MapZipWithLambda
            extends LambdaFunctionInterface
    {
        Object apply(Object key, Object value1, Object value2);
    }
}
