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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.function.Function;

import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

public class TaskOutputOperator
        implements Operator
{
    public static class TaskOutputFactory
            implements OutputFactory
    {
        private final OutputBuffer outputBuffer;

        public TaskOutputFactory(OutputBuffer outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId, List<Type> types, Function<Page, Page> pagePreprocessor, PagesSerdeFactory serdeFactory)
        {
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pagePreprocessor, serdeFactory);
        }
    }

    public static class TaskOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final OutputBuffer outputBuffer;
        private final Function<Page, Page> pagePreprocessor;
        private final PagesSerdeFactory serdeFactory;

        public TaskOutputOperatorFactory(int operatorId, PlanNodeId planNodeId, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor, PagesSerdeFactory serdeFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TaskOutputOperator.class.getSimpleName());
            return new TaskOutputOperator(operatorContext, outputBuffer, pagePreprocessor, serdeFactory);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pagePreprocessor, serdeFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final OutputBuffer outputBuffer;
    private final Function<Page, Page> pagePreprocessor;
    private final PagesSerde serde;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;
    private boolean finished;

    public TaskOutputOperator(OperatorContext operatorContext, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor, PagesSerdeFactory serdeFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid re-synchronizing on the output buffer when operator is already blocked
        if (isBlocked.isDone()) {
            isBlocked = outputBuffer.isFull();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);

        outputBuffer.enqueue(splitAndSerializePage(page));
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private List<Slice> splitAndSerializePage(Page page)
    {
        List<Page> split = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        ImmutableList.Builder<Slice> builder = ImmutableList.builderWithExpectedSize(split.size());
        try (PagesSerde.PagesSerdeContext context = serde.newContext()) {
            for (Page p : split) {
                builder.add(serde.serialize(context, p));
            }
        }
        return builder.build();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
