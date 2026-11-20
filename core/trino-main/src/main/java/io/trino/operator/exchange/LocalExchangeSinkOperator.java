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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.ReferenceCount;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSinkOperator
        implements Operator
{
    public static class LocalExchangeSinkOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final LocalExchangeSinkFactory sinkFactory;
        private final PlanNodeId planNodeId;
        private final Function<Page, Page> pagePreprocessor;
        private final List<Integer> inputChannels;
        private final ReferenceCount factoryReferenceCount;
        private boolean closed;

        public LocalExchangeSinkOperatorFactory(LocalExchangeSinkFactory sinkFactory, int operatorId, PlanNodeId planNodeId, Function<Page, Page> pagePreprocessor)
        {
            this(sinkFactory, operatorId, planNodeId, pagePreprocessor, List.of(), new ReferenceCount(1));
            factoryReferenceCount.getFreeFuture().addListener(sinkFactory::noMoreSinkFactories, directExecutor());
        }

        public LocalExchangeSinkOperatorFactory(LocalExchangeSinkFactory sinkFactory, int operatorId, PlanNodeId planNodeId, Function<Page, Page> pagePreprocessor, List<Integer> inputChannels)
        {
            this(sinkFactory, operatorId, planNodeId, pagePreprocessor, inputChannels, new ReferenceCount(1));
            factoryReferenceCount.getFreeFuture().addListener(sinkFactory::noMoreSinkFactories, directExecutor());
        }

        private LocalExchangeSinkOperatorFactory(LocalExchangeSinkFactory sinkFactory, int operatorId, PlanNodeId planNodeId, Function<Page, Page> pagePreprocessor, List<Integer> inputChannels, ReferenceCount factoryReferenceCount)
        {
            this.sinkFactory = requireNonNull(sinkFactory, "sinkFactory is null");
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.inputChannels = List.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
            this.factoryReferenceCount = requireNonNull(factoryReferenceCount, "factoryReferenceCount is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSinkOperator.class.getSimpleName());

            return new LocalExchangeSinkOperator(operatorContext, sinkFactory.createSink(), pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Already closed");
            closed = true;
            sinkFactory.close();
            factoryReferenceCount.release();
        }

        @Override
        public OperatorFactory duplicate()
        {
            factoryReferenceCount.retain();
            return new LocalExchangeSinkOperatorFactory(sinkFactory.duplicate(), operatorId, planNodeId, pagePreprocessor, inputChannels, factoryReferenceCount);
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            if (!request.channelsMatch(channel -> channel < inputChannels.size())) {
                context.stop(this, request);
                return;
            }
            input.accept(request.mapChannels(inputChannels::get));
        }

        @Override
        public void registerRuntimeConstraintInput(Consumer<List<RuntimeConstraintRequest>> requests, RuntimeConstraintWiringContext context)
        {
            context.registerLocalConsumer(sinkFactory.getLocalExchange(), requests);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalExchangeSink sink;
    private final Function<Page, Page> pagePreprocessor;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

    LocalExchangeSinkOperator(OperatorContext operatorContext, LocalExchangeSink sink, Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        operatorContext.setFinishedFuture(sink.isFinished());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        sink.finish();
    }

    @Override
    public boolean isFinished()
    {
        return sink.isFinished().isDone();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (isBlocked.isDone()) {
            isBlocked = sink.waitForWriting();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        page = pagePreprocessor.apply(page);
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
        sink.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        finish();
    }
}
