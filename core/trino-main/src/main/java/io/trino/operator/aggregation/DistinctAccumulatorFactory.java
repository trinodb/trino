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
import io.trino.operator.MarkDistinctHash;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class DistinctAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorFactory delegate;
    private final List<Type> argumentTypes;
    private final JoinCompiler joinCompiler;
    private final TypeOperators typeOperators;
    private final Session session;

    public DistinctAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> argumentTypes,
            JoinCompiler joinCompiler,
            TypeOperators typeOperators,
            Session session)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
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
                typeOperators);
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
                typeOperators);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return delegate.createGroupedIntermediateAccumulator(lambdaProviders);
    }

    @Override
    public AggregationMaskBuilder createAggregationMaskBuilder()
    {
        return delegate.createAggregationMaskBuilder();
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
                TypeOperators typeOperators)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hash = new MarkDistinctHash(
                    session,
                    inputTypes,
                    false,
                    joinCompiler,
                    typeOperators,
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
        public void addInput(Page arguments, AggregationMask mask)
        {
            // 1. filter out positions based on mask, if present
            Page filtered = mask.filterPage(arguments);

            // 2. compute a distinct mask block
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. update original mask to the new distinct mask block
            mask.reset(filtered.getPositionCount());
            mask.applyMaskBlock(distinctMask);
            if (mask.isSelectNone()) {
                return;
            }

            // 4. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(filtered, mask);
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
                TypeOperators typeOperators)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hash = new MarkDistinctHash(
                    session,
                    ImmutableList.<Type>builder()
                            .add(INTEGER) // group id column
                            .addAll(inputTypes)
                            .build(),
                    false,
                    joinCompiler,
                    typeOperators,
                    UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public void setGroupCount(long groupCount)
        {
            accumulator.setGroupCount(groupCount);
        }

        @Override
        public void addInput(int[] groupIds, Page page, AggregationMask mask)
        {
            // 1. filter out positions based on mask
            groupIds = maskGroupIds(groupIds, mask);
            page = mask.filterPage(page);

            // 2. compute a mask for the distinct rows (including the group id)
            Work<Block> work = hash.markDistinctRows(page.prependColumn(new IntArrayBlock(page.getPositionCount(), Optional.empty(), groupIds)));
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. update original mask to the new distinct mask block
            mask.reset(page.getPositionCount());
            mask.applyMaskBlock(distinctMask);
            if (mask.isSelectNone()) {
                return;
            }

            // 4. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(groupIds, page, mask);
        }

        private static int[] maskGroupIds(int[] groupIds, AggregationMask mask)
        {
            if (mask.isSelectAll() || mask.isSelectNone()) {
                return groupIds;
            }

            int[] newGroupIds = new int[mask.getSelectedPositionCount()];
            int[] selectedPositions = mask.getSelectedPositions();
            for (int i = 0; i < newGroupIds.length; i++) {
                newGroupIds[i] = groupIds[selectedPositions[i]];
            }
            return newGroupIds;
        }

        @Override
        public void addIntermediate(int[] groupIds, Block block)
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
}
