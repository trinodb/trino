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
import io.trino.spi.function.ColumnarScalarFunctionImplementation;
import io.trino.sql.ir.Expression;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class ColumnarScalarFunctionPageProjection
        implements PageProjection
{
    private final Expression expression;
    private final boolean deterministic;
    private final InputChannels inputChannels;
    private final List<Argument> arguments;
    private final ColumnarScalarFunctionImplementation implementation;

    public ColumnarScalarFunctionPageProjection(
            Expression expression,
            boolean deterministic,
            InputChannels inputChannels,
            List<Argument> arguments,
            ColumnarScalarFunctionImplementation implementation)
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
        SourcePage argumentsPage = new ArgumentsSourcePage(page, selectedPositions, arguments);
        Block result = requireNonNull(implementation.evaluate(session, argumentsPage), "Columnar scalar function returned null");
        verify(result.getPositionCount() == selectedPositions.size(),
                "Columnar scalar function returned %s positions, expected %s",
                result.getPositionCount(),
                selectedPositions.size());
        return result;
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

    static final class ArgumentsSourcePage
            implements SourcePage
    {
        private static final long INSTANCE_SIZE = instanceSize(ArgumentsSourcePage.class);

        private final SourcePage sourcePage;
        private final SelectedPositions selectedPositions;
        private final List<Argument> arguments;
        private final Block[] blocks;

        ArgumentsSourcePage(SourcePage sourcePage, SelectedPositions selectedPositions, List<Argument> arguments)
        {
            this.sourcePage = requireNonNull(sourcePage, "sourcePage is null");
            this.selectedPositions = requireNonNull(selectedPositions, "selectedPositions is null");
            this.arguments = requireNonNull(arguments, "arguments is null");
            this.blocks = new Block[arguments.size()];
        }

        @Override
        public int getPositionCount()
        {
            return selectedPositions.size();
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
                block = selectPositions(sourcePage.getBlock(argument.inputChannel()), selectedPositions);
            }
            else {
                block = RunLengthEncodedBlock.create(argument.constant(), selectedPositions.size());
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
            throw new UnsupportedOperationException("Columnar scalar function arguments are already selected");
        }

        private static Block selectPositions(Block block, SelectedPositions selectedPositions)
        {
            if (selectedPositions.isList()) {
                return block.getPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
            }
            if (selectedPositions.getOffset() == 0 && selectedPositions.size() == block.getPositionCount()) {
                return block;
            }
            return block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
    }
}
