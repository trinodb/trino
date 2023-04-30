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
package io.trino.spi.block;

import io.trino.spi.type.ArrayType;

import static io.airlift.slice.SizeOf.instanceSize;

public class BufferedArrayValueBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(BufferedArrayValueBuilder.class);

    private int bufferSize;
    private BlockBuilder valueBuilder;

    public static BufferedArrayValueBuilder createBuffered(ArrayType arrayType)
    {
        return new BufferedArrayValueBuilder(arrayType, 1024);
    }

    BufferedArrayValueBuilder(ArrayType arrayType, int bufferSize)
    {
        this.bufferSize = bufferSize;
        this.valueBuilder = arrayType.getElementType().createBlockBuilder(null, bufferSize);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + valueBuilder.getRetainedSizeInBytes();
    }

    public <E extends Throwable> Block build(int entryCount, ArrayValueBuilder<E> builder)
            throws E
    {
        // grow or reset builders if necessary
        if (valueBuilder.getPositionCount() + entryCount > bufferSize) {
            if (bufferSize < entryCount) {
                bufferSize = entryCount;
            }
            valueBuilder = valueBuilder.newBlockBuilderLike(bufferSize, null);
        }

        int startSize = valueBuilder.getPositionCount();

        builder.build(valueBuilder);

        int endSize = valueBuilder.getPositionCount();
        return valueBuilder.build().getRegion(startSize, endSize - startSize);
    }
}
