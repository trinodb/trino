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
package io.trino.operator.join.unspilled;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.JoinStatisticsCounter;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.unspilled.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.WorkProcessor.TransformationState.blocked;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.operator.WorkProcessor.TransformationState.yielded;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.FULL_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class PageJoiner
        implements WorkProcessor.Transformation<Page, Page>, Closeable
{
    private final JoinProbeFactory joinProbeFactory;
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final JoinStatisticsCounter statisticsCounter;
    private final DriverYieldSignal yieldSignal;
    private final LookupJoinPageBuilder pageBuilder;
    private final boolean probeOnOuterSide;
    private final boolean outputSingleMatch;

    @Nullable
    private LookupSource lookupSource;
    @Nullable
    private JoinProbe probe;
    private long joinPosition = -1;
    private int joinSourcePositions;
    private boolean currentProbePositionProducedRow;

    public PageJoiner(
            ProcessorContext processorContext,
            List<Type> buildOutputTypes,
            JoinType joinType,
            boolean outputSingleMatch,
            JoinProbeFactory joinProbeFactory,
            ListenableFuture<LookupSource> lookupSource,
            JoinStatisticsCounter statisticsCounter)
    {
        requireNonNull(processorContext, "processorContext is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.lookupSourceFuture = requireNonNull(lookupSource, "lookupSource is null");
        this.statisticsCounter = requireNonNull(statisticsCounter, "statisticsCounter is null");
        this.yieldSignal = processorContext.getDriverYieldSignal();
        this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
        this.outputSingleMatch = outputSingleMatch;

        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
    }

    @Override
    public void close()
    {
        pageBuilder.reset();
        addSuccessCallback(lookupSourceFuture, LookupSource::close);
    }

    @Override
    public WorkProcessor.TransformationState<Page> process(@Nullable Page probePage)
    {
        boolean finishing = probePage == null;

        if (probe == null && finishing) {
            close();
            return finished();
        }

        if (lookupSource == null) {
            if (!lookupSourceFuture.isDone()) {
                return blocked(asVoid(lookupSourceFuture));
            }

            lookupSource = requireNonNull(getDone(lookupSourceFuture));
            statisticsCounter.updateLookupSourcePositions(lookupSource.getJoinPositionCount());
        }
        if (probe == null) {
            probe = joinProbeFactory.createJoinProbe(probePage, lookupSource);
            statisticsCounter.recordCreateProbe();
        }

        processProbe(lookupSource);

        if (!probe.isFinished()) {
            // processProbe() returns when pageBuilder is full or yield signal is triggered.

            if (pageBuilder.isFull()) {
                return ofResult(buildOutputPage(), false);
            }

            return yielded();
        }

        if (!pageBuilder.isEmpty() || finishing) {
            // flush the current page (possibly empty one) and reset probe
            Page outputPage = buildOutputPage();
            probe = null;
            return ofResult(outputPage, !finishing);
        }

        probe = null;
        return needsMoreData();
    }

    private void processProbe(LookupSource lookupSource)
    {
        do {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, yieldSignal)) {
                    break;
                }
                if (probeOnOuterSide && !outerJoinCurrentPosition()) {
                    break;
                }
                statisticsCounter.recordProbe(joinSourcePositions);
            }

            if (handleRleProbe()) {
                break;
            }

            if (!advanceProbePosition()) {
                break;
            }
        }
        while (!yieldSignal.isSet());
    }

    /**
     * @return true if run length encoded probe has been handled and probe page processing is now finished
     */
    private boolean handleRleProbe()
    {
        if (!probe.areProbeJoinChannelsRunLengthEncoded()) {
            return false;
        }

        if (probe.getPosition() != 0) {
            // RLE probe can be handled only after first row is processed
            return false;
        }

        if (pageBuilder.getPositionCount() == 0) {
            // skip matching of other probe rows since first
            // row from RLE probe did not produce any matches
            probe.finish();
            statisticsCounter.recordRleProbe();
            return true;
        }

        if (pageBuilder.getPositionCount() == 1) {
            // repeat probe join key match
            pageBuilder.repeatBuildRow();
            probe.finish();
            statisticsCounter.recordRleProbe();
            return true;
        }

        // process probe row by row since there are multiple matches per probe join key
        return false;
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, DriverYieldSignal yieldSignal)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.appendRow(probe, lookupSource, joinPosition);
                joinSourcePositions++;
            }

            if (outputSingleMatch && currentProbePositionProducedRow) {
                joinPosition = -1;
            }
            else {
                // get next position on lookup side for this probe row
                joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());
            }

            if (yieldSignal.isSet() || pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side.
     *
     * @return whether pageBuilder can still not fill
     */
    private boolean outerJoinCurrentPosition()
    {
        if (!currentProbePositionProducedRow) {
            currentProbePositionProducedRow = true;
            pageBuilder.appendNullForBuild(probe);
            return !pageBuilder.isFull();
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        // reset row join state for next row
        joinSourcePositions = 0;
        currentProbePositionProducedRow = false;
        return true;
    }

    private Page buildOutputPage()
    {
        verifyNotNull(probe);
        Page outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
        return outputPage;
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
