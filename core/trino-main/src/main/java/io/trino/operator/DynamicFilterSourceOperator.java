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
package io.trino.operator;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * This operator acts as a simple "pass-through" pipe, while saving a summary of input pages.
 * The collected values are used for creating a run-time filtering constraint (for probe-side table scan in an inner join).
 * We record all values for the run-time filter only for small build-side pages (which should be the case when using "broadcast" join).
 * For large inputs on the build side, we can optionally record the min and max values per channel for orderable types (except Double and Real).
 */
public class DynamicFilterSourceOperator
        implements Operator
{
    public record Channel(DynamicFilterId filterId, Type type, int index) {}

    public static class DynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final DynamicFilterSourceConsumer dynamicPredicateConsumer;
        private final List<Channel> channels;
        private final int maxDistinctValues;
        private final DataSize maxFilterSize;
        private final int minMaxCollectionLimit;
        private final TypeOperators typeOperators;

        private boolean closed;
        private int createdOperatorsCount;

        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                DynamicFilterSourceConsumer dynamicPredicateConsumer,
                List<Channel> channels,
                int maxDistinctValues,
                DataSize maxFilterSize,
                int minMaxCollectionLimit,
                TypeOperators typeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
            this.channels = requireNonNull(channels, "channels is null");
            verify(channels.stream().map(Channel::filterId).collect(toSet()).size() == channels.size(),
                    "duplicate dynamic filters are not allowed");
            verify(channels.stream().map(Channel::index).collect(toSet()).size() == channels.size(),
                    "duplicate channel indices are not allowed");
            this.maxDistinctValues = maxDistinctValues;
            this.maxFilterSize = maxFilterSize;
            this.minMaxCollectionLimit = minMaxCollectionLimit;
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            createdOperatorsCount++;
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterSourceOperator.class.getSimpleName());
            if (!dynamicPredicateConsumer.isDomainCollectionComplete()) {
                return new DynamicFilterSourceOperator(
                        operatorContext,
                        dynamicPredicateConsumer,
                        channels,
                        maxDistinctValues,
                        maxFilterSize,
                        minMaxCollectionLimit,
                        typeOperators);
            }
            // Return a pass-through operator which adds little overhead
            return new PassthroughDynamicFilterSourceOperator(operatorContext);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
            dynamicPredicateConsumer.setPartitionCount(createdOperatorsCount);
        }

        @Override
        public OperatorFactory duplicate()
        {
            // A duplicate factory may be required for DynamicFilterSourceOperatorFactory in fault-tolerant execution mode
            // by LocalExecutionPlanner#addLookupOuterDrivers to add a new driver to output the unmatched rows in an outer join.
            // Since the logic for tracking partitions count for dynamicPredicateConsumer requires there to be only one DynamicFilterSourceOperatorFactory,
            // we turn off dynamic filtering and provide a duplicate factory which will act as pass through to allow the query to succeed.
            dynamicPredicateConsumer.addPartition(TupleDomain.all());
            return new DynamicFilterSourceOperatorFactory(
                    operatorId,
                    planNodeId,
                    new DynamicFilterSourceConsumer() {
                        @Override
                        public void addPartition(TupleDomain<DynamicFilterId> tupleDomain)
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setPartitionCount(int partitionCount) {}

                        @Override
                        public boolean isDomainCollectionComplete()
                        {
                            return true;
                        }
                    },
                    channels,
                    maxDistinctValues,
                    maxFilterSize,
                    minMaxCollectionLimit,
                    typeOperators);
        }
    }

    private final OperatorContext context;
    private final LocalMemoryContext userMemoryContext;
    private boolean finished;
    private Page current;
    private final DynamicFilterSourceConsumer dynamicPredicateConsumer;

    private final List<Channel> channels;
    private final JoinDomainBuilder[] joinDomainBuilders;

    private int minMaxCollectionLimit;
    private boolean isDomainCollectionComplete;

    private DynamicFilterSourceOperator(
            OperatorContext context,
            DynamicFilterSourceConsumer dynamicPredicateConsumer,
            List<Channel> channels,
            int maxDistinctValues,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            TypeOperators typeOperators)
    {
        this.context = requireNonNull(context, "context is null");
        this.userMemoryContext = context.localUserMemoryContext();
        this.minMaxCollectionLimit = minMaxCollectionLimit;
        this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
        this.channels = requireNonNull(channels, "channels is null");

        this.joinDomainBuilders = channels.stream()
                .map(Channel::type)
                .map(type -> new JoinDomainBuilder(
                        type,
                        maxDistinctValues,
                        maxFilterSize,
                        minMaxCollectionLimit > 0,
                        this::finishDomainCollectionIfNecessary,
                        typeOperators))
                .toArray(JoinDomainBuilder[]::new);

        userMemoryContext.setBytes(stream(joinDomainBuilders)
                .mapToLong(JoinDomainBuilder::getRetainedSizeInBytes)
                .sum());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() may not be called after finish()");
        current = page;

        if (isDomainCollectionComplete) {
            return;
        }

        if (minMaxCollectionLimit >= 0) {
            minMaxCollectionLimit -= page.getPositionCount();
            if (minMaxCollectionLimit < 0) {
                for (int channelIndex = 0; channelIndex < channels.size(); channelIndex++) {
                    joinDomainBuilders[channelIndex].disableMinMax();
                }
                finishDomainCollectionIfNecessary();
            }
        }

        // Collect only the columns which are relevant for the JOIN.
        long retainedSize = 0;
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            Block block = page.getBlock(channels.get(channelIndex).index());
            joinDomainBuilders[channelIndex].add(block);
            if (isDomainCollectionComplete) {
                return;
            }
            retainedSize += joinDomainBuilders[channelIndex].getRetainedSizeInBytes();
        }
        userMemoryContext.setBytes(retainedSize);
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;
        if (isDomainCollectionComplete) {
            // dynamicPredicateConsumer was already notified with 'all'
            return;
        }

        ImmutableMap.Builder<DynamicFilterId, Domain> domainsBuilder = ImmutableMap.builder();
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            DynamicFilterId filterId = channels.get(channelIndex).filterId();
            domainsBuilder.put(filterId, joinDomainBuilders[channelIndex].build());
        }
        dynamicPredicateConsumer.addPartition(TupleDomain.withColumnDomains(domainsBuilder.buildOrThrow()));
        userMemoryContext.setBytes(0);
        Arrays.fill(joinDomainBuilders, null);
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }

    @Override
    public void close()
            throws Exception
    {
        userMemoryContext.setBytes(0);
    }

    private void finishDomainCollectionIfNecessary()
    {
        if (!isDomainCollectionComplete && stream(joinDomainBuilders).noneMatch(JoinDomainBuilder::isCollecting)) {
            // allow all probe-side values to be read.
            dynamicPredicateConsumer.addPartition(TupleDomain.all());
            isDomainCollectionComplete = true;
            userMemoryContext.setBytes(0);
        }
    }

    private static class PassthroughDynamicFilterSourceOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private boolean finished;
        private Page current;

        private PassthroughDynamicFilterSourceOperator(OperatorContext operatorContext)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public boolean needsInput()
        {
            return current == null && !finished;
        }

        @Override
        public void addInput(Page page)
        {
            verify(!finished, "DynamicFilterSourceOperator: addInput() may not be called after finish()");
            current = page;
        }

        @Override
        public Page getOutput()
        {
            Page result = current;
            current = null;
            return result;
        }

        @Override
        public void finish()
        {
            if (finished) {
                // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
                return;
            }
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return current == null && finished;
        }
    }
}
