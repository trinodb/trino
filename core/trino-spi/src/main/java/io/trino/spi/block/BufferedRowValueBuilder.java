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

import io.trino.spi.type.RowType;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;

public class BufferedRowValueBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(BufferedRowValueBuilder.class);

    private final int bufferSize;
    private List<BlockBuilder> fieldBuilders;

    public static BufferedRowValueBuilder createBuffered(RowType rowType)
    {
        return new BufferedRowValueBuilder(rowType, 1024);
    }

    BufferedRowValueBuilder(RowType rowType, int bufferSize)
    {
        this.bufferSize = bufferSize;
        this.fieldBuilders = rowType.getTypeParameters().stream()
                .map(fieldType -> fieldType.createBlockBuilder(null, bufferSize))
                .toList();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + fieldBuilders.stream().mapToLong(BlockBuilder::getRetainedSizeInBytes).sum();
    }

    public <E extends Throwable> Block build(RowValueBuilder<E> builder)
            throws E
    {
        int expectedSize = fieldBuilders.get(0).getPositionCount();
        if (!fieldBuilders.stream().allMatch(field -> field.getPositionCount() == expectedSize)) {
            // we could fix this by appending nulls to the shorter builders, but this is a sign the buffer is being used in a multithreaded environment which is not supported
            throw new IllegalStateException("Field builders were corrupted by a previous call to buildValue");
        }

        // grow or reset builders if necessary
        if (fieldBuilders.get(0).getPositionCount() + 1 > bufferSize) {
            fieldBuilders = fieldBuilders.stream()
                    .map(field -> field.newBlockBuilderLike(bufferSize, null))
                    .toList();
        }

        int startSize = fieldBuilders.get(0).getPositionCount();

        try {
            builder.build(fieldBuilders);
        }
        catch (Exception e) {
            equalizeBlockBuilders();
            throw e;
        }

        // check that field builders have the same size
        if (equalizeBlockBuilders()) {
            throw new IllegalStateException("Expected field builders to have the same size");
        }
        int endSize = fieldBuilders.get(0).getPositionCount();
        if (endSize != startSize + 1) {
            throw new IllegalStateException("Expected exactly one entry added to each field builder");
        }

        List<Block> blocks = fieldBuilders.stream()
                .map(field -> field.build().getRegion(startSize, 1))
                .toList();
        return new SingleRowBlock(0, blocks.toArray(new Block[0]));
    }

    private boolean equalizeBlockBuilders()
    {
        // append nulls to even out the blocks
        boolean nullsAppended = false;
        int newBlockSize = fieldBuilders.stream().mapToInt(BlockBuilder::getPositionCount).max().orElseThrow();
        for (BlockBuilder fieldBuilder : fieldBuilders) {
            while (fieldBuilder.getPositionCount() < newBlockSize) {
                fieldBuilder.appendNull();
                nullsAppended = true;
            }
        }
        return nullsAppended;
    }
}
