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
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
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

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapZipWithFunction.class, "mapZipWith", Type.class, Type.class, Type.class, MapType.class, Object.class, Block.class, Block.class, MapZipWithLambda.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(MapZipWithFunction.class, "createState", MapType.class);

    private MapZipWithFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("map_zip_with")
                        .typeVariable("K")
                        .typeVariable("V1")
                        .typeVariable("V2")
                        .typeVariable("V3")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V3")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V1")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V2")))
                        .argumentType(functionType(new TypeSignature("K"), new TypeSignature("V1"), new TypeSignature("V2"), new TypeSignature("V3")))
                        .build())
                .nondeterministic()
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
        return new PageBuilder(ImmutableList.of(mapType));
    }

    public static Block mapZipWith(
            Type keyType,
            Type leftValueType,
            Type rightValueType,
            MapType outputMapType,
            Object state,
            Block leftBlock,
            Block rightBlock,
            MapZipWithLambda function)
    {
        SingleMapBlock leftMapBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightMapBlock = (SingleMapBlock) rightBlock;
        Type outputValueType = outputMapType.getValueType();

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        // seekKey() can take non-trivial time when key is complicated value, such as a long VARCHAR or ROW.
        boolean[] keyFound = new boolean[rightMapBlock.getPositionCount()];
        for (int leftKeyPosition = 0; leftKeyPosition < leftMapBlock.getPositionCount(); leftKeyPosition += 2) {
            Object key = readNativeValue(keyType, leftMapBlock, leftKeyPosition);
            Object leftValue = readNativeValue(leftValueType, leftMapBlock, leftKeyPosition + 1);

            int rightValuePosition = rightMapBlock.seekKey(key);
            Object rightValue = null;
            if (rightValuePosition != -1) {
                rightValue = readNativeValue(rightValueType, rightMapBlock, rightValuePosition);
                keyFound[rightValuePosition / 2] = true;
            }

            Object outputValue;
            try {
                outputValue = function.apply(key, leftValue, rightValue);
            }
            catch (Throwable throwable) {
                // Restore pageBuilder into a consistent state.
                mapBlockBuilder.closeEntry();
                pageBuilder.declarePosition();

                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }

            keyType.appendTo(leftMapBlock, leftKeyPosition, blockBuilder);
            writeNativeValue(outputValueType, blockBuilder, outputValue);
        }

        // iterate over keys that only exists in rightMapBlock
        for (int rightKeyPosition = 0; rightKeyPosition < rightMapBlock.getPositionCount(); rightKeyPosition += 2) {
            if (!keyFound[rightKeyPosition / 2]) {
                Object key = readNativeValue(keyType, rightMapBlock, rightKeyPosition);
                Object rightValue = readNativeValue(rightValueType, rightMapBlock, rightKeyPosition + 1);

                Object outputValue;
                try {
                    outputValue = function.apply(key, null, rightValue);
                }
                catch (Throwable throwable) {
                    // Restore pageBuilder into a consistent state.
                    mapBlockBuilder.closeEntry();
                    pageBuilder.declarePosition();

                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }

                keyType.appendTo(rightMapBlock, rightKeyPosition, blockBuilder);
                writeNativeValue(outputValueType, blockBuilder, outputValue);
            }
        }

        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return outputMapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    @FunctionalInterface
    public interface MapZipWithLambda
            extends LambdaFunctionInterface
    {
        Object apply(Object key, Object value1, Object value2);
    }
}
