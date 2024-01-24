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

import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LastNOperator
        implements Operator
{
    public static class LastNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int count;
        private boolean closed;

        public LastNOperatorFactory(int operatorId, PlanNodeId planNodeId, int count)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.count = count;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LastNOperator.class.getSimpleName());
            return new LastNOperator(operatorContext, count);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LastNOperatorFactory(operatorId, planNodeId, count);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private long count;
    private long totalPositions;
    private State state = State.NEEDS_INPUT;
    private long lastPageRowGroupId;
    private final ArrayDeque<Page> outputPages;
    private boolean canOutput;
    private int remainingLimit;
    private ArrayDeque<Page> currentQueue;
    private ArrayDeque<ArrayDeque<Page>> totalQueue;

    public LastNOperator(OperatorContext operatorContext, int count)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(count >= 0, "count must be at least zero");
        this.count = count;
        this.remainingLimit = count;
        this.currentQueue = new ArrayDeque<>();
        this.totalQueue = new ArrayDeque<>();
        this.outputPages = new ArrayDeque<>();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        state = State.FINISHED;
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED && totalQueue == null;
    }

    @Override
    public boolean needsInput()
    {
        return count > 0 && state != State.FINISHED;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        long currPageRowGroupId = getCurrentPageRowGroupId(page);
        if (currPageRowGroupId != lastPageRowGroupId) {
            totalPositions = 0;
            if (!currentQueue.isEmpty()) {
                totalQueue.add(reverseDeque(currentQueue));
                int countInLastRowGroup = 0;
                for (Page currPage : currentQueue) {
                    countInLastRowGroup += currPage.getPositionCount();
                }
                count = count - countInLastRowGroup;
                currentQueue = new ArrayDeque<>();
            }
        }
        lastPageRowGroupId = currPageRowGroupId;
        totalPositions += page.getPositionCount();
        currentQueue.add(page);
        while (!currentQueue.isEmpty() && totalPositions - currentQueue.peek().getPositionCount() >= count) {
            totalPositions -= currentQueue.remove().getPositionCount();
        }
    }

    private ArrayDeque<Page> reverseDeque(ArrayDeque<Page> deque)
    {
        ArrayDeque<Page> reversedDeque = new ArrayDeque<>();
        Iterator<Page> descendingIterator = deque.descendingIterator();
        while (descendingIterator.hasNext()) {
            reversedDeque.add(descendingIterator.next());
        }
        return reversedDeque;
    }

    private long getCurrentPageRowGroupId(Page page)
    {
        return page.getBlock(page.getChannelCount() - 1).getLong(0, 0);
    }

    @Override
    public Page getOutput()
    {
        if (count > 0 && state != State.FINISHED) {
            return null;
        }
        if (!canOutput) {
            if (!currentQueue.isEmpty()) {
                totalQueue.add(reverseDeque(currentQueue));
            }
            ArrayDeque<Page> pages = totalQueue.stream().flatMap(Collection::stream).collect(Collectors.toCollection(ArrayDeque::new));
            for (Page page : pages) {
                if (page.getPositionCount() <= remainingLimit) {
                    remainingLimit = remainingLimit - page.getPositionCount();
                    outputPages.add(page);
                }
                else {
                    Page region = page.getRegion(page.getPositionCount() - remainingLimit, remainingLimit);
                    outputPages.add(region);
                }
            }
            canOutput = true;
        }
        if (outputPages.isEmpty()) {
            state = State.FINISHED;
            totalQueue = null;
            return null;
        }
        return outputPages.removeFirst();
    }

    @Override
    public void close()
            throws Exception
    {
        totalQueue = null;
    }
}
