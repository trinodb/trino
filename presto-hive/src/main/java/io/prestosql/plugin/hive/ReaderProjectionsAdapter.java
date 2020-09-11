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
package io.prestosql.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.ReaderProjectionsAdapter.ChannelMapping.createChannelMapping;
import static io.prestosql.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

public class ReaderProjectionsAdapter
{
    private final List<ChannelMapping> outputToInputMapping;
    private final List<Type> outputTypes;
    private final List<Type> inputTypes;

    public ReaderProjectionsAdapter(List<HiveColumnHandle> expectedHiveColumns, ReaderProjections readerProjections)
    {
        requireNonNull(expectedHiveColumns, "expectedHiveColumns is null");
        requireNonNull(readerProjections, "readerProjections is null");

        ImmutableList.Builder<ChannelMapping> mappingBuilder = ImmutableList.builder();

        for (int i = 0; i < expectedHiveColumns.size(); i++) {
            HiveColumnHandle projectedColumnHandle = readerProjections.readerColumnForHiveColumnAt(i);
            int inputChannel = readerProjections.readerColumnPositionForHiveColumnAt(i);
            ChannelMapping mapping = createChannelMapping(expectedHiveColumns.get(i), projectedColumnHandle, inputChannel);
            mappingBuilder.add(mapping);
        }

        outputToInputMapping = mappingBuilder.build();

        outputTypes = expectedHiveColumns.stream()
                .map(HiveColumnHandle::getType)
                .collect(toImmutableList());

        inputTypes = readerProjections.getReaderColumns().stream()
                .map(HiveColumnHandle::getType)
                .collect(toImmutableList());
    }

    public Page adaptPage(Page input)
    {
        if (input == null) {
            return null;
        }

        Block[] blocks = new Block[outputToInputMapping.size()];

        // Prepare adaptations to extract dereferences
        for (int i = 0; i < outputToInputMapping.size(); i++) {
            ChannelMapping mapping = outputToInputMapping.get(i);

            Block inputBlock = input.getBlock(mapping.getInputChannelIndex());
            blocks[i] = createAdaptedLazyBlock(inputBlock, mapping.getDereferenceSequence(), outputTypes.get(i));
        }

        return new Page(input.getPositionCount(), blocks);
    }

    private static Block createAdaptedLazyBlock(Block inputBlock, List<Integer> dereferenceSequence, Type type)
    {
        if (dereferenceSequence.size() == 0) {
            return inputBlock;
        }

        if (inputBlock == null) {
            return null;
        }

        return new LazyBlock(inputBlock.getPositionCount(), new DereferenceBlockLoader(inputBlock, dereferenceSequence, type));
    }

    private static class DereferenceBlockLoader
            implements LazyBlockLoader
    {
        private final List<Integer> dereferenceSequence;
        private final Type type;
        private boolean loaded;
        private Block inputBlock;

        DereferenceBlockLoader(Block inputBlock, List<Integer> dereferenceSequence, Type type)
        {
            this.inputBlock = requireNonNull(inputBlock, "inputBlock is null");
            this.dereferenceSequence = requireNonNull(dereferenceSequence, "dereferenceSequence is null");
            this.type = type;
        }

        @Override
        public Block load()
        {
            checkState(!loaded, "Already loaded");
            Block loadedBlock = loadInternalBlock(dereferenceSequence, inputBlock);
            inputBlock = null;
            loaded = true;
            return loadedBlock;
        }

        /**
         * Applies dereference operations on the input block to extract the required internal block. If the input block is lazy
         * in a nested manner, this implementation avoids loading the entire input block.
         */
        private Block loadInternalBlock(List<Integer> dereferences, Block parentBlock)
        {
            if (dereferences.size() == 0) {
                return parentBlock.getLoadedBlock();
            }

            ColumnarRow columnarRow = toColumnarRow(parentBlock);

            int dereferenceIndex = dereferences.get(0);
            List<Integer> remainingDereferences = dereferences.subList(1, dereferences.size());

            Block fieldBlock = columnarRow.getField(dereferenceIndex);
            Block loadedInternalBlock = loadInternalBlock(remainingDereferences, fieldBlock);

            // Field blocks provided by ColumnarRow can have a smaller position count, because they do not store nulls.
            // The following step adds null elements (when required) to the loaded block.
            return adaptNulls(columnarRow, loadedInternalBlock);
        }

        private Block adaptNulls(ColumnarRow columnarRow, Block loadedInternalBlock)
        {
            // TODO: The current implementation copies over data to a new block builder when a null row element is found.
            //  We can optimize this by using a Block implementation that uses a null vector of the parent row block and
            //  the block for the field.

            BlockBuilder newlyCreatedBlock = null;
            int fieldBlockPosition = 0;

            for (int i = 0; i < columnarRow.getPositionCount(); i++) {
                boolean isRowNull = columnarRow.isNull(i);

                if (isRowNull) {
                    // A new block is only created when a null is encountered for the first time.
                    if (newlyCreatedBlock == null) {
                        newlyCreatedBlock = type.createBlockBuilder(null, columnarRow.getPositionCount());

                        // Copy over all elements encountered so far to the new block
                        for (int j = 0; j < i; j++) {
                            type.appendTo(loadedInternalBlock, j, newlyCreatedBlock);
                        }
                    }
                    newlyCreatedBlock.appendNull();
                }
                else {
                    if (newlyCreatedBlock != null) {
                        type.appendTo(loadedInternalBlock, fieldBlockPosition, newlyCreatedBlock);
                    }
                    fieldBlockPosition++;
                }
            }

            if (newlyCreatedBlock == null) {
                // If there was no need to create a null, return the original block
                return loadedInternalBlock;
            }

            return newlyCreatedBlock.build();
        }
    }

    List<ChannelMapping> getOutputToInputMapping()
    {
        return outputToInputMapping;
    }

    List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    List<Type> getInputTypes()
    {
        return inputTypes;
    }

    @VisibleForTesting
    static class ChannelMapping
    {
        private final int inputChannelIndex;
        private final List<Integer> dereferenceSequence;

        private ChannelMapping(int inputBlockIndex, List<Integer> dereferenceSequence)
        {
            checkArgument(inputBlockIndex >= 0, "inputBlockIndex cannot be negative");
            this.inputChannelIndex = inputBlockIndex;
            this.dereferenceSequence = ImmutableList.copyOf(requireNonNull(dereferenceSequence, "dereferences is null"));
        }

        public int getInputChannelIndex()
        {
            return inputChannelIndex;
        }

        public List<Integer> getDereferenceSequence()
        {
            return dereferenceSequence;
        }

        static ChannelMapping createChannelMapping(HiveColumnHandle expected, HiveColumnHandle delegate, int inputBlockIndex)
        {
            List<Integer> dereferences = validateProjectionAndExtractDereferences(expected, delegate);
            return new ChannelMapping(inputBlockIndex, dereferences);
        }

        private static List<Integer> validateProjectionAndExtractDereferences(HiveColumnHandle expectedColumn, HiveColumnHandle readerColumn)
        {
            checkArgument(expectedColumn.getBaseColumn().equals(readerColumn.getBaseColumn()), "reader column is not valid for expected column");

            List<Integer> expectedDereferences = expectedColumn.getHiveColumnProjectionInfo()
                    .map(HiveColumnProjectionInfo::getDereferenceIndices)
                    .orElse(ImmutableList.of());

            List<Integer> readerDereferences = readerColumn.getHiveColumnProjectionInfo()
                    .map(HiveColumnProjectionInfo::getDereferenceIndices)
                    .orElse(ImmutableList.of());

            checkArgument(readerDereferences.size() <= expectedDereferences.size(), "Field returned by the reader should include expected field");
            checkArgument(expectedDereferences.subList(0, readerDereferences.size()).equals(readerDereferences), "Field returned by the reader should be a prefix of expected field");

            return expectedDereferences.subList(readerDereferences.size(), expectedDereferences.size());
        }
    }
}
