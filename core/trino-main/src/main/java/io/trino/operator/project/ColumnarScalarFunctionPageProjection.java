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
package io.trino.operator.project;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.ColumnarScalarFunction;
import io.trino.sql.ir.Expression;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.function.SelectedPositions.positionsList;
import static io.trino.spi.function.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

public final class ColumnarScalarFunctionPageProjection
        implements PageProjection
{
    private final Expression expression;
    private final boolean deterministic;
    private final InputChannels inputChannels;
    private final List<Argument> arguments;
    private final ColumnarScalarFunction implementation;

    public ColumnarScalarFunctionPageProjection(
            Expression expression,
            boolean deterministic,
            InputChannels inputChannels,
            List<Argument> arguments,
            ColumnarScalarFunction implementation)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.deterministic = deterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.arguments = List.copyOf(arguments);
        this.implementation = requireNonNull(implementation, "implementation is null");

        for (Argument argument : arguments) {
            if (argument.isInput()) {
                checkArgument(argument.inputChannel() < inputChannels.size(), "Invalid input channel: %s", argument.inputChannel());
            }
        }
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        Block result;
        if (selectedPositions.isList()) {
            result = projectPositionsList(session, page, selectedPositions);
        }
        else {
            int size = selectedPositions.size();
            result = evaluate(session, page, selectedPositions.getOffset(), size, positionsRange(0, size));
        }
        verify(result.getPositionCount() == selectedPositions.size(),
                "Columnar scalar function returned %s positions, expected %s",
                result.getPositionCount(),
                selectedPositions.size());
        return result;
    }

    /**
     * The function arguments are the region of the page between the first and the last selected
     * position, so they can be passed without copying, and the work of a function that reuses the
     * structure of its arguments is bounded by this region. Selected positions are passed to the
     * function relative to the start of this region. When the region covers most of the page, the
     * whole page is passed instead, with the selected positions unchanged.
     */
    private Block projectPositionsList(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        int size = selectedPositions.size();
        if (size == 0) {
            return evaluate(session, page, 0, 0, positionsRange(0, 0));
        }

        int[] positions = selectedPositions.getPositions();
        int offset = selectedPositions.getOffset();
        int min = positions[offset];
        int max = positions[offset];
        boolean ascending = true;
        for (int index = offset + 1; index < offset + size; index++) {
            int position = positions[index];
            ascending &= position > positions[index - 1];
            min = Math.min(min, position);
            max = Math.max(max, position);
        }
        int length = max - min + 1;
        if (ascending && length == size) {
            return evaluate(session, page, min, size, positionsRange(0, size));
        }
        // A region that covers at least 7/8 of the page bounds little work, so the page is passed with the selected
        // positions as they are, which avoids copying them
        int pagePositionCount = page.getPositionCount();
        int wholePageLength = pagePositionCount - (pagePositionCount >> 3);
        if (length >= wholePageLength) {
            return evaluate(session, page, 0, pagePositionCount, positionsList(positions, offset, size));
        }

        int[] relativePositions = new int[size];
        for (int index = 0; index < size; index++) {
            relativePositions[index] = positions[offset + index] - min;
        }
        return evaluate(session, page, min, length, positionsList(relativePositions, 0, size));
    }

    private Block evaluate(ConnectorSession session, SourcePage page, int start, int length, io.trino.spi.function.SelectedPositions selectedPositions)
    {
        SourcePage argumentsPage = new ArgumentsSourcePage(page, start, length, arguments);
        return requireNonNull(implementation.evaluate(session, argumentsPage, selectedPositions), "Columnar scalar function returned null");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .toString();
    }

    public record Argument(int inputChannel, @Nullable Block constant)
    {
        public Argument
        {
            checkArgument((inputChannel >= 0) != (constant != null), "Argument must be an input or a constant");
        }

        public static Argument input(int inputChannel)
        {
            return new Argument(inputChannel, null);
        }

        public static Argument constant(Block constant)
        {
            requireNonNull(constant, "constant is null");
            checkArgument(constant.getPositionCount() == 1, "Constant must contain exactly one position");
            return new Argument(-1, constant);
        }

        public boolean isInput()
        {
            return inputChannel >= 0;
        }
    }

    private static final class ArgumentsSourcePage
            implements SourcePage
    {
        private static final long INSTANCE_SIZE = instanceSize(ArgumentsSourcePage.class);

        private final SourcePage sourcePage;
        private final int start;
        private final int length;
        private final List<Argument> arguments;
        private final Block[] blocks;

        public ArgumentsSourcePage(SourcePage sourcePage, int start, int length, List<Argument> arguments)
        {
            this.sourcePage = requireNonNull(sourcePage, "sourcePage is null");
            this.start = start;
            this.length = length;
            this.arguments = requireNonNull(arguments, "arguments is null");
            this.blocks = new Block[arguments.size()];
        }

        @Override
        public int getPositionCount()
        {
            return length;
        }

        @Override
        public long getSizeInBytes()
        {
            long size = 0;
            for (Block block : blocks) {
                if (block != null) {
                    size += block.getSizeInBytes();
                }
            }
            return size;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            long size = INSTANCE_SIZE + sizeOf(blocks);
            for (Block block : blocks) {
                if (block != null) {
                    size += block.getRetainedSizeInBytes();
                }
            }
            return size;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            consumer.accept(this, INSTANCE_SIZE);
            consumer.accept(blocks, sizeOf(blocks));
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return arguments.size();
        }

        @Override
        public Block getBlock(int channel)
        {
            Block block = blocks[channel];
            if (block != null) {
                return block;
            }

            Argument argument = arguments.get(channel);
            if (argument.isInput()) {
                block = sourcePage.getBlock(argument.inputChannel());
                if (start != 0 || length != block.getPositionCount()) {
                    block = block.getRegion(start, length);
                }
            }
            else {
                block = RunLengthEncodedBlock.create(argument.constant(), length);
            }
            blocks[channel] = block;
            return block;
        }

        @Override
        public Page getPage()
        {
            for (int channel = 0; channel < blocks.length; channel++) {
                getBlock(channel);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            throw new UnsupportedOperationException("Columnar scalar function arguments cannot be selected");
        }
    }
}
