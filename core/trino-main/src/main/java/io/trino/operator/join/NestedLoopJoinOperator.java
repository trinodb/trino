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
package io.trino.operator.join;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.Lifespan;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;

public class NestedLoopJoinOperator
        implements Operator
{
    public static class NestedLoopJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<NestedLoopJoinBridge> joinBridgeManager;
        private final List<Integer> probeChannels;
        private final List<Integer> buildChannels;
        private boolean closed;

        public NestedLoopJoinOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager,
                List<Integer> probeChannels,
                List<Integer> buildChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = nestedLoopJoinBridgeManager;
            this.joinBridgeManager.incrementProbeFactoryCount();
            this.probeChannels = ImmutableList.copyOf(requireNonNull(probeChannels, "probeChannels is null"));
            this.buildChannels = ImmutableList.copyOf(requireNonNull(buildChannels, "buildChannels is null"));
        }

        private NestedLoopJoinOperatorFactory(NestedLoopJoinOperatorFactory other)
        {
            requireNonNull(other, "other is null");
            this.operatorId = other.operatorId;
            this.planNodeId = other.planNodeId;

            this.joinBridgeManager = other.joinBridgeManager;

            this.probeChannels = ImmutableList.copyOf(other.probeChannels);
            this.buildChannels = ImmutableList.copyOf(other.buildChannels);

            // closed is intentionally not copied
            closed = false;

            joinBridgeManager.incrementProbeFactoryCount();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            NestedLoopJoinBridge nestedLoopJoinBridge = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopJoinOperator.class.getSimpleName());

            joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
            return new NestedLoopJoinOperator(
                    operatorContext,
                    nestedLoopJoinBridge,
                    probeChannels,
                    buildChannels,
                    () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()));
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
            joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.probeOperatorFactoryClosed(lifespan);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopJoinOperatorFactory(this);
        }
    }

    private final ListenableFuture<NestedLoopJoinPages> nestedLoopJoinPagesFuture;
    private final ListenableFuture<Void> blockedFutureView;

    private final OperatorContext operatorContext;
    private final Runnable afterClose;

    private final int[] probeChannels;
    private final int[] buildChannels;
    private List<Page> buildPages;
    private Page probePage;
    private Iterator<Page> buildPageIterator;
    private NestedLoopOutputIterator nestedLoopPageBuilder;
    private boolean finishing;
    private boolean closed;

    private NestedLoopJoinOperator(OperatorContext operatorContext, NestedLoopJoinBridge joinBridge, List<Integer> probeChannels, List<Integer> buildChannels, Runnable afterClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinPagesFuture = joinBridge.getPagesFuture();
        blockedFutureView = asVoid(nestedLoopJoinPagesFuture);
        this.probeChannels = Ints.toArray(requireNonNull(probeChannels, "probeChannels is null"));
        this.buildChannels = Ints.toArray(requireNonNull(buildChannels, "buildChannels is null"));
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && probePage == null;

        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return blockedFutureView;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || probePage != null) {
            return false;
        }

        if (buildPages == null) {
            Optional<NestedLoopJoinPages> nestedLoopJoinPages = tryGetFutureValue(nestedLoopJoinPagesFuture);
            if (nestedLoopJoinPages.isPresent()) {
                buildPages = nestedLoopJoinPages.get().getPages();
            }
        }
        return buildPages != null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(buildPages != null, "Page source has not been built yet");
        checkState(probePage == null, "Current page has not been completely processed yet");
        checkState(buildPageIterator == null || !buildPageIterator.hasNext(), "Current buildPageIterator has not been completely processed yet");

        if (page.getPositionCount() > 0) {
            probePage = page;
            buildPageIterator = buildPages.iterator();
        }
    }

    @Override
    public Page getOutput()
    {
        // Either probe side or build side is not ready
        if (probePage == null || buildPages == null) {
            return null;
        }

        if (nestedLoopPageBuilder != null && nestedLoopPageBuilder.hasNext()) {
            return nestedLoopPageBuilder.next();
        }

        if (buildPageIterator.hasNext()) {
            nestedLoopPageBuilder = createNestedLoopOutputIterator(probePage, buildPageIterator.next(), probeChannels, buildChannels);
            return nestedLoopPageBuilder.next();
        }

        probePage = null;
        nestedLoopPageBuilder = null;
        return null;
    }

    @Override
    public void close()
    {
        buildPages = null;
        probePage = null;
        nestedLoopPageBuilder = null;
        buildPageIterator = null;
        // We don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        // `afterClose` must be run last.
        afterClose.run();
    }

    @VisibleForTesting
    static NestedLoopOutputIterator createNestedLoopOutputIterator(Page probePage, Page buildPage, int[] probeChannels, int[] buildChannels)
    {
        if (probeChannels.length == 0 && buildChannels.length == 0) {
            int probePositions = probePage.getPositionCount();
            int buildPositions = buildPage.getPositionCount();
            try {
                // positionCount is an int. Make sure the product can still fit in an int.
                int outputPositions = multiplyExact(probePositions, buildPositions);
                if (outputPositions <= PageProcessor.MAX_BATCH_SIZE) {
                    return new PageRepeatingIterator(new Page(outputPositions), 1);
                }
            }
            catch (ArithmeticException overflow) {
            }
            // Repeat larger position count a smaller position count number of times
            Page outputPage = new Page(max(probePositions, buildPositions));
            return new PageRepeatingIterator(outputPage, min(probePositions, buildPositions));
        }
        else if (probeChannels.length == 0 && probePage.getPositionCount() <= buildPage.getPositionCount()) {
            return new PageRepeatingIterator(buildPage.getColumns(buildChannels), probePage.getPositionCount());
        }
        else if (buildChannels.length == 0 && buildPage.getPositionCount() <= probePage.getPositionCount()) {
            return new PageRepeatingIterator(probePage.getColumns(probeChannels), buildPage.getPositionCount());
        }
        else {
            return new NestedLoopPageBuilder(probePage, buildPage, probeChannels, buildChannels);
        }
    }

    // bi-morphic parent class for the two implementations allowed. Adding a third implementation will make getOutput megamorphic and
    // should be avoided
    @VisibleForTesting
    abstract static class NestedLoopOutputIterator
    {
        public abstract boolean hasNext();

        public abstract Page next();
    }

    private static final class PageRepeatingIterator
            extends NestedLoopOutputIterator
    {
        private final Page page;
        private int remainingCount;

        private PageRepeatingIterator(Page page, int repetitions)
        {
            this.page = requireNonNull(page, "page is null");
            this.remainingCount = repetitions;
        }

        @Override
        public boolean hasNext()
        {
            return remainingCount > 0;
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            remainingCount--;
            return page;
        }
    }

    /**
     * This class takes one probe page(p rows) and one build page(b rows) and
     * build n pages with m rows in each page, where n = min(p, b) and m = max(p, b)
     */
    private static final class NestedLoopPageBuilder
            extends NestedLoopOutputIterator
    {
        //  Avoids allocation a new block array per iteration
        private final Block[] resultBlockBuffer;
        private final Block[] smallPageOutputBlocks;
        private final int indexForRleBlocks;
        private final int largePagePositionCount;
        private final int maxRowIndex; // number of rows - 1

        private int rowIndex = -1; // Iterator on the rows in the page with less rows.

        NestedLoopPageBuilder(Page probePage, Page buildPage, int[] probeChannels, int[] buildChannels)
        {
            requireNonNull(probePage, "probePage is null");
            checkArgument(probePage.getPositionCount() > 0, "probePage has no rows");
            requireNonNull(buildPage, "buildPage is null");
            checkArgument(buildPage.getPositionCount() > 0, "buildPage has no rows");

            Page largePage;
            Page smallPage;
            int indexForPageBlocks;
            int[] channelsForSmallPage;
            int[] channelsForLargePage;
            if (buildPage.getPositionCount() > probePage.getPositionCount()) {
                largePage = buildPage;
                smallPage = probePage;
                channelsForLargePage = buildChannels;
                channelsForSmallPage = probeChannels;
                indexForPageBlocks = probeChannels.length;
                this.indexForRleBlocks = 0;
            }
            else {
                largePage = probePage;
                smallPage = buildPage;
                channelsForLargePage = probeChannels;
                channelsForSmallPage = buildChannels;
                indexForPageBlocks = 0;
                this.indexForRleBlocks = probeChannels.length;
            }
            this.largePagePositionCount = largePage.getPositionCount();
            this.maxRowIndex = smallPage.getPositionCount() - 1;

            this.resultBlockBuffer = new Block[channelsForLargePage.length + channelsForSmallPage.length];
            // Put the blocks from the page with more rows in the output buffer in order
            for (int i = 0; i < channelsForLargePage.length; i++) {
                resultBlockBuffer[indexForPageBlocks + i] = largePage.getBlock(channelsForLargePage[i]);
            }
            // Extract the small page output blocks in order
            this.smallPageOutputBlocks = new Block[channelsForSmallPage.length];
            for (int i = 0; i < channelsForSmallPage.length; i++) {
                this.smallPageOutputBlocks[i] = smallPage.getBlock(channelsForSmallPage[i]);
            }
        }

        @Override
        public boolean hasNext()
        {
            return rowIndex < maxRowIndex;
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            rowIndex++;

            // For the page with less rows, create RLE blocks and add them to the blocks array
            for (int i = 0; i < smallPageOutputBlocks.length; i++) {
                Block block = smallPageOutputBlocks[i].getSingleValueBlock(rowIndex);
                resultBlockBuffer[indexForRleBlocks + i] = new RunLengthEncodedBlock(block, largePagePositionCount);
            }
            // Page constructor will create a copy of the block buffer (and must for correctness)
            return new Page(largePagePositionCount, resultBlockBuffer);
        }
    }
}
