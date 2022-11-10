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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.MarkDistinctHash;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class DistinctAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorFactory delegate;
    private final List<Type> argumentTypes;
    private final JoinCompiler joinCompiler;
    private final BlockTypeOperators blockTypeOperators;
    private final Session session;

    public DistinctAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> argumentTypes,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            Session session)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return delegate.getLambdaInterfaces();
    }

    @Override
    public Accumulator createAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return new DistinctAccumulator(
                delegate.createAccumulator(lambdaProviders),
                argumentTypes,
                session,
                joinCompiler,
                blockTypeOperators);
    }

    @Override
    public Accumulator createIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return delegate.createIntermediateAccumulator(lambdaProviders);
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return new DistinctGroupedAccumulator(
                delegate.createGroupedAccumulator(lambdaProviders),
                argumentTypes,
                session,
                joinCompiler,
                blockTypeOperators);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return delegate.createGroupedIntermediateAccumulator(lambdaProviders);
    }

    private static class DistinctAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final MarkDistinctHash hash;

        private DistinctAccumulator(
                Accumulator accumulator,
                List<Type> inputTypes,
                Session session,
                JoinCompiler joinCompiler,
                BlockTypeOperators blockTypeOperators)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hash = new MarkDistinctHash(
                    session,
                    inputTypes,
                    IntStream.range(0, inputTypes.size()).toArray(),
                    Optional.empty(),
                    joinCompiler,
                    blockTypeOperators,
                    UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public Accumulator copy()
        {
            throw new UnsupportedOperationException("Distinct aggregation function state can not be copied");
        }

        @Override
        public void addInput(Page arguments, Optional<Block> mask)
        {
            // 1. filter out positions based on mask, if present
            Page filtered = mask
                    .map(maskBlock -> filter(arguments, maskBlock))
                    .orElse(arguments);

            if (filtered.getPositionCount() == 0) {
                return;
            }

            // 2. compute a distinct mask
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(filtered, Optional.of(distinctMask));
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static class DistinctGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final MarkDistinctHash hash;

        private DistinctGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> inputTypes,
                Session session,
                JoinCompiler joinCompiler,
                BlockTypeOperators blockTypeOperators)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hash = new MarkDistinctHash(
                    session,
                    ImmutableList.<Type>builder()
                            .add(BIGINT) // group id column
                            .addAll(inputTypes)
                            .build(),
                    IntStream.range(0, inputTypes.size() + 1).toArray(),
                    Optional.empty(),
                    joinCompiler,
                    blockTypeOperators,
                    UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page, Optional<Block> mask)
        {
            Page withGroup = page.prependColumn(groupIdsBlock);

            // 1. filter out positions based on mask, if present
            Page filteredWithGroup = mask
                    .map(maskBlock -> filter(withGroup, maskBlock))
                    .orElse(withGroup);

            // 2. compute a mask for the distinct rows (including the group id)
            Work<Block> work = hash.markDistinctRows(filteredWithGroup);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            GroupByIdBlock groupIds = new GroupByIdBlock(groupIdsBlock.getGroupCount(), filteredWithGroup.getBlock(0));

            // drop the group id column and prepend the distinct mask column
            int[] columnIndexes = new int[filteredWithGroup.getChannelCount() - 1];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnIndexes[i] = i + 1;
            }
            Page filtered = filteredWithGroup.getColumns(columnIndexes);
            // NOTE: the accumulator must be called even if the filtered page is empty to inform the accumulator about the group count
            accumulator.addInput(groupIds, filtered, Optional.of(distinctMask));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal() {}
    }

    private static Page filter(Page page, Block mask)
    {
        int positions = mask.getPositionCount();
        if (positions > 0 && mask instanceof RunLengthEncodedBlock) {
            // must have at least 1 position to be able to check the value at position 0
            if (!mask.isNull(0) && BOOLEAN.getBoolean(mask, 0)) {
                return page;
            }
            return page.getPositions(new int[0], 0, 0);
        }
        boolean mayHaveNull = mask.mayHaveNull();
        int[] ids = new int[positions];
        int next = 0;
        for (int i = 0; i < ids.length; ++i) {
            boolean isNull = mayHaveNull && mask.isNull(i);
            if (!isNull && BOOLEAN.getBoolean(mask, i)) {
                ids[next++] = i;
            }
        }

        if (next == ids.length) {
            return page; // no rows were eliminated by the filter
        }
        return page.getPositions(ids, 0, next);
    }
}
