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
package io.trino.operator.function;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState.Blocked;
import io.trino.spi.function.table.TableFunctionProcessorState.Finished;
import io.trino.spi.function.table.TableFunctionProcessorState.Processed;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Processes the input of a table function having a single source with row semantics.
 * The result of such a function is determined on a row-by-row basis, so the input does not have
 * to be buffered. A single {@link TableFunctionDataProcessor} gets the input pages as they arrive.
 * <p>
 * Pass-through indexes are counted from the beginning of the input and must refer to the input
 * page being processed, or be null when there is none. Pass-through columns are views of that
 * page, or copies when the result has fewer than three quarters of the rows of the page or
 * contains a null index.
 */
public class StreamTableFunctionInput
        implements TableFunctionInput
{
    // position of a null pass-through index
    private static final int NULL_INDEX = -1;

    private final WorkProcessor<Page> inputPages;
    private final Supplier<TableFunctionDataProcessor> processorSupplier;
    private final int properChannelsCount;
    private final int passThroughSourcesCount;
    private final int[] requiredChannels;
    private final int[] passThroughChannels;
    // channel of the pass-through indexes in the page returned by the function
    private final int passThroughIndexChannel;
    private final List<Type> sourceTypes;
    private final LocalMemoryContext memoryContext;

    private TableFunctionDataProcessor processor;
    // index of the first row of the current input page, from the beginning of the input
    private long inputPageStart;

    public StreamTableFunctionInput(
            WorkProcessor<Page> inputPages,
            Supplier<TableFunctionDataProcessor> processorSupplier,
            int properChannelsCount,
            int passThroughSourcesCount,
            List<Integer> requiredChannels,
            List<PassThroughColumnSpecification> passThroughSpecifications,
            List<Type> sourceTypes,
            AggregatedMemoryContext memoryContext)
    {
        this.inputPages = requireNonNull(inputPages, "inputPages is null");
        this.processorSupplier = requireNonNull(processorSupplier, "processorSupplier is null");
        this.properChannelsCount = properChannelsCount;
        this.passThroughSourcesCount = passThroughSourcesCount;
        this.requiredChannels = Ints.toArray(requireNonNull(requiredChannels, "requiredChannels is null"));
        requireNonNull(passThroughSpecifications, "passThroughSpecifications is null");
        checkArgument(
                passThroughSpecifications.stream().noneMatch(PassThroughColumnSpecification::isPartitioningColumn),
                "partitioning columns are not expected for a source with row semantics");
        checkArgument(
                passThroughSpecifications.stream().mapToInt(PassThroughColumnSpecification::indexChannel).distinct().count() <= 1,
                "pass-through columns of a single source must share the index channel");
        this.passThroughChannels = passThroughSpecifications.stream()
                .mapToInt(PassThroughColumnSpecification::inputChannel)
                .toArray();
        this.passThroughIndexChannel = passThroughSpecifications.isEmpty() ? -1 : passThroughSpecifications.getFirst().indexChannel();
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.memoryContext = memoryContext.newLocalMemoryContext(StreamTableFunctionInput.class.getSimpleName());
    }

    @Override
    public WorkProcessor<Page> toOutputPages()
    {
        return inputPages.transform(this::process);
    }

    /**
     * @param inputPage the input page being processed, or null when the input is finished
     */
    private TransformationState<Page> process(@Nullable Page inputPage)
    {
        if (processor == null) {
            if (inputPage == null) {
                // a row-semantics source is pruned when empty, so there is nothing to process
                return finished();
            }
            processor = processorSupplier.get();
        }
        // the input page is retained until the function reports it as used
        memoryContext.setBytes(inputPage == null ? 0 : inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> input = inputPage == null ? null : ImmutableList.of(Optional.of(inputPage.getColumns(requiredChannels)));
        return switch (processor.process(input)) {
            case Finished _ -> finished();
            case Blocked blocked -> blocked(blocked);
            case Processed processed -> processed(processed, inputPage);
        };
    }

    private TransformationState<Page> finished()
    {
        memoryContext.close();
        return TransformationState.finished();
    }

    private static TransformationState<Page> blocked(Blocked blocked)
    {
        return TransformationState.blocked(toListenableFuture(blocked.getFuture()));
    }

    private TransformationState<Page> processed(Processed processed, @Nullable Page inputPage)
    {
        if (processed.getResult() == null) {
            if (inputPage == null) {
                throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "When function got no input, it should either produce output or return Blocked state");
            }
            completeInputPage(inputPage);
            return TransformationState.needsMoreData();
        }

        // pass-through indexes refer to the input page, so append before completing it
        Page output = appendPassThroughColumns(processed.getResult(), inputPage);
        boolean inputConsumed = inputPage != null && processed.isUsedInput();
        if (inputConsumed) {
            completeInputPage(inputPage);
        }
        return TransformationState.ofResult(output, inputConsumed);
    }

    private void completeInputPage(Page inputPage)
    {
        inputPageStart += inputPage.getPositionCount();
        memoryContext.setBytes(0);
    }

    private Page appendPassThroughColumns(Page result, @Nullable Page inputPage)
    {
        if (result.getChannelCount() != properChannelsCount + passThroughSourcesCount) {
            throw new TrinoException(
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Table function returned a page containing %s channels. Expected channel number: %s (%s proper columns, %s pass-through index columns)".formatted(
                            result.getChannelCount(),
                            properChannelsCount + passThroughSourcesCount,
                            properChannelsCount,
                            passThroughSourcesCount));
        }

        Block[] resultBlocks = new Block[properChannelsCount + passThroughChannels.length];

        for (int channel = 0; channel < properChannelsCount; channel++) {
            resultBlocks[channel] = result.getBlock(channel);
        }

        if (passThroughChannels.length > 0) {
            Block indexes = result.getBlock(passThroughIndexChannel);
            Block[] passThroughColumns;
            if (inputPage == null) {
                passThroughColumns = nullPassThroughColumns(indexes);
            }
            else {
                passThroughColumns = passThroughColumns(indexes, inputPage);
            }
            System.arraycopy(passThroughColumns, 0, resultBlocks, properChannelsCount, passThroughColumns.length);
        }

        // pass the position count, as there may be no output channels
        return new Page(result.getPositionCount(), resultBlocks);
    }

    /**
     * Without an input page there are no rows to pass through, so all indexes must be null.
     */
    private Block[] nullPassThroughColumns(Block indexes)
    {
        for (int position = 0; position < indexes.getPositionCount(); position++) {
            if (!indexes.isNull(position)) {
                throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Index of a pass-through row must be null when no input data is being processed. Actual: " + BIGINT.getLong(indexes, position));
            }
        }
        Block[] columns = new Block[passThroughChannels.length];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = RunLengthEncodedBlock.create(sourceTypes.get(passThroughChannels[i]), null, indexes.getPositionCount());
        }
        return columns;
    }

    private Block[] passThroughColumns(Block indexes, Page inputPage)
    {
        int[] positions = toInputPositions(indexes, inputPage);
        if (indexes.hasNull()) {
            return copyPassThroughColumns(inputPage, positions);
        }
        // rows can differ in size, so the row count only approximates how much of the page is used;
        // keep a view only when the result has at least three quarters of the rows
        boolean copy = positions.length * 4L < inputPage.getPositionCount() * 3L;
        Block[] columns = new Block[passThroughChannels.length];
        for (int i = 0; i < columns.length; i++) {
            Block block = inputPage.getBlock(passThroughChannels[i]);
            columns[i] = copy ? block.copyPositions(positions, 0, positions.length) : block.getPositions(positions, 0, positions.length);
        }
        return columns;
    }

    private int[] toInputPositions(Block indexes, Page inputPage)
    {
        int[] positions = new int[indexes.getPositionCount()];
        for (int position = 0; position < positions.length; position++) {
            if (indexes.isNull(position)) {
                positions[position] = NULL_INDEX;
            }
            else {
                positions[position] = toPositionInPage(BIGINT.getLong(indexes, position), inputPage);
            }
        }
        return positions;
    }

    private int toPositionInPage(long index, Page inputPage)
    {
        long positionInPage = index - inputPageStart;
        if (positionInPage < 0 || positionInPage >= inputPage.getPositionCount()) {
            throw new TrinoException(
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Index of a pass-through row: %s out of the input page being processed [%s, %s]".formatted(
                            index,
                            inputPageStart,
                            inputPageStart + inputPage.getPositionCount() - 1));
        }
        return toIntExact(positionInPage);
    }

    /**
     * Appends row by row, because {@link Block#copyWithAppendedNull()} would copy the whole
     * input page for every result.
     */
    private Block[] copyPassThroughColumns(Page inputPage, int[] positions)
    {
        Block[] copies = new Block[passThroughChannels.length];
        for (int i = 0; i < copies.length; i++) {
            Block block = inputPage.getBlock(passThroughChannels[i]);
            BlockBuilder builder = sourceTypes.get(passThroughChannels[i]).createBlockBuilder(null, positions.length);
            for (int position : positions) {
                if (position == NULL_INDEX) {
                    builder.appendNull();
                }
                else {
                    builder.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
                }
            }
            copies[i] = builder.build();
        }
        return copies;
    }
}
