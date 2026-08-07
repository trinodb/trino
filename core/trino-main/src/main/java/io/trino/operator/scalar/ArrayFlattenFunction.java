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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.function.ColumnarScalarImplementation;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static java.lang.Math.toIntExact;

@ScalarFunction("flatten")
@Description("Flattens the given array")
public final class ArrayFlattenFunction
{
    private ArrayFlattenFunction() {}

    @ColumnarScalarImplementation
    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block flattenColumnar(@SqlType("array(array(E))") Block arrayColumn)
    {
        ColumnarArray outerArrays = ColumnarArray.toColumnarArray(arrayColumn);
        ColumnarArray innerArrays = ColumnarArray.toColumnarArray(outerArrays.getElementsBlock());

        int positionCount = outerArrays.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        long[] valueIsValid = outerArrays.mayHaveNull() ? allocateWords(positionCount, false) : null;
        for (int position = 0; position < positionCount; position++) {
            if (!outerArrays.isNull(position)) {
                if (valueIsValid != null) {
                    set(valueIsValid, 0, position);
                }
                int innerOffset = outerArrays.getOffset(position);
                int innerCount = outerArrays.getLength(position);
                int elementCount = 0;
                for (int innerPosition = innerOffset; innerPosition < innerOffset + innerCount; innerPosition++) {
                    elementCount += innerArrays.getLength(innerPosition);
                }
                offsets[position + 1] = offsets[position] + elementCount;
            }
            else {
                offsets[position + 1] = offsets[position];
            }
        }
        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, innerArrays.getElementsBlock());
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block flatten(
            @TypeParameter("E") Type type,
            @TypeParameter("array(E)") Type arrayType,
            @SqlType("array(array(E))") Block array)
    {
        if (array.getPositionCount() == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        BlockBuilder builder = type.createBlockBuilder(null, array.getPositionCount(), toIntExact(array.getSizeInBytes() / array.getPositionCount()));
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                Block subArray = (Block) arrayType.getObject(array, i);
                builder.appendBlockRange(subArray, 0, subArray.getPositionCount());
            }
        }
        return builder.build();
    }
}
