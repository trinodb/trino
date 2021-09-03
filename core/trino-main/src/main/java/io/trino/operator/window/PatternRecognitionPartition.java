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
package io.trino.operator.window;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.PagesIndex;
import io.trino.operator.window.Framing.Range;
import io.trino.operator.window.matcher.ArrayView;
import io.trino.operator.window.matcher.MatchResult;
import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.pattern.LabelEvaluator;
import io.trino.operator.window.pattern.LabelEvaluator.Evaluation;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MeasureComputation;
import io.trino.spi.PageBuilder;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowIndex;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.SkipTo;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static java.lang.Math.max;

public final class PatternRecognitionPartition
        implements WindowPartition
{
    private final PagesIndex pagesIndex;
    private final WindowIndex windowIndex;
    private final int partitionStart;
    private final int partitionEnd;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final PagesHashStrategy peerGroupHashStrategy;
    private final LocalMemoryContext matcherMemoryContext;

    private int peerGroupStart;
    private int peerGroupEnd;

    private int currentPosition;

    // properties for row pattern recognition
    private final List<MeasureComputation> measures;
    private final Optional<RowsFraming> framing;
    private final PatternRecognitionRelation.RowsPerMatch rowsPerMatch;
    private final Optional<LogicalIndexNavigation> skipToNavigation;
    private final SkipTo.Position skipToPosition;
    private final boolean initial;
    private final Matcher matcher;
    private final List<Evaluation> labelEvaluations;

    private int lastSkippedPosition;
    private int lastMatchedPosition;
    private long matchNumber;

    public PatternRecognitionPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            PagesHashStrategy peerGroupHashStrategy,
            AggregatedMemoryContext memoryContext,
            List<MeasureComputation> measures,
            Optional<FrameInfo> commonBaseFrame,
            PatternRecognitionRelation.RowsPerMatch rowsPerMatch,
            Optional<LogicalIndexNavigation> skipToNavigation,
            SkipTo.Position skipToPosition,
            boolean initial,
            Matcher matcher,
            List<Evaluation> labelEvaluations)
    {
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.outputChannels = outputChannels;
        this.windowFunctions = ImmutableList.copyOf(windowFunctions);
        this.peerGroupHashStrategy = peerGroupHashStrategy;
        this.matcherMemoryContext = memoryContext.newLocalMemoryContext(Matcher.class.getSimpleName());
        this.measures = ImmutableList.copyOf(measures);
        this.framing = commonBaseFrame.map(frameInfo -> new RowsFraming(frameInfo, partitionStart, partitionEnd, pagesIndex));
        this.rowsPerMatch = rowsPerMatch;
        this.skipToNavigation = skipToNavigation;
        this.skipToPosition = skipToPosition;
        this.initial = initial;
        this.matcher = matcher;
        this.labelEvaluations = ImmutableList.copyOf(labelEvaluations);

        this.lastSkippedPosition = partitionStart - 1;
        this.lastMatchedPosition = partitionStart - 1;
        this.matchNumber = 1;

        // reset functions for new partition
        this.windowIndex = new PagesWindowIndex(pagesIndex, partitionStart, partitionEnd);
        for (WindowFunction windowFunction : windowFunctions) {
            windowFunction.reset(windowIndex);
        }

        currentPosition = partitionStart;
        updatePeerGroup();
    }

    @Override
    public int getPartitionStart()
    {
        return partitionStart;
    }

    @Override
    public int getPartitionEnd()
    {
        return partitionEnd;
    }

    @Override
    public boolean hasNext()
    {
        return currentPosition < partitionEnd;
    }

    @Override
    public void processNextRow(PageBuilder pageBuilder)
    {
        checkState(hasNext(), "No more rows in partition");

        // check for new peer group
        if (currentPosition == peerGroupEnd) {
            updatePeerGroup();
        }

        if (isSkipped(currentPosition)) {
            // this position was skipped by AFTER MATCH SKIP of some previous row. no pattern match is attempted and frame is empty.
            if (rowsPerMatch == WINDOW) {
                outputUnmatchedRow(pageBuilder);
            }
        }
        else {
            // try match pattern from the current row on
            // 1. determine pattern search boundaries.
            //    - for MATCH_RECOGNIZE, pattern matching and associated computations can involve the whole partition
            //    - for WINDOW, pattern matching and associated computations are limited to the "full frame", represented by commonBaseFrame. It is specified as "ROWS BETWEEN CURRENT ROW AND ..."
            int searchStart = partitionStart;
            int searchEnd = partitionEnd;
            int patternStart = currentPosition;
            if (framing.isPresent()) {
                // the currentGroup parameter does not apply to frame type ROWS
                Range baseRange = framing.get().getRange(currentPosition, -1, peerGroupStart, peerGroupEnd);
                searchStart = partitionStart + baseRange.getStart();
                searchEnd = partitionStart + baseRange.getEnd() + 1;
            }
            LabelEvaluator labelEvaluator = new LabelEvaluator(matchNumber, patternStart, partitionStart, searchStart, searchEnd, labelEvaluations, windowIndex);
            MatchResult matchResult = matcher.run(labelEvaluator, matcherMemoryContext);

            // 2. in case SEEK was specified (as opposite to INITIAL), try match pattern starting from subsequent rows until the first match is found
            while (!matchResult.isMatched() && !initial && patternStart < searchEnd - 1) {
                patternStart++;
                labelEvaluator = new LabelEvaluator(matchNumber, patternStart, partitionStart, searchStart, searchEnd, labelEvaluations, windowIndex);
                matchResult = matcher.run(labelEvaluator, matcherMemoryContext);
            }

            // produce output depending on match and output mode (rowsPerMatch)
            if (!matchResult.isMatched()) {
                if (rowsPerMatch == WINDOW || (rowsPerMatch.isUnmatchedRows() && !isMatched(currentPosition))) {
                    outputUnmatchedRow(pageBuilder);
                }
                lastSkippedPosition = currentPosition;
            }
            else if (matchResult.getLabels().length() == 0) {
                if (rowsPerMatch.isEmptyMatches()) {
                    outputEmptyMatch(pageBuilder);
                }
                lastSkippedPosition = currentPosition;
                matchNumber++;
            }
            else { // non-empty match
                if (rowsPerMatch.isOneRow()) {
                    outputOneRowPerMatch(pageBuilder, matchResult, patternStart, searchStart, searchEnd);
                }
                else {
                    outputAllRowsPerMatch(pageBuilder, matchResult, searchStart, searchEnd);
                }
                updateLastMatchedPosition(matchResult, patternStart);
                skipAfterMatch(matchResult, patternStart, searchStart, searchEnd);
                matchNumber++;
            }
        }

        currentPosition++;
    }

    private boolean isSkipped(int position)
    {
        return position <= lastSkippedPosition;
    }

    private boolean isMatched(int position)
    {
        return position <= lastMatchedPosition;
    }

    // the output for unmatched row refers to no pattern match and empty frame.
    private void outputUnmatchedRow(PageBuilder pageBuilder)
    {
        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // measures are all null for no match
        for (int i = 0; i < measures.size(); i++) {
            pageBuilder.getBlockBuilder(channel).appendNull();
            channel++;
        }
        // window functions have empty frame
        for (WindowFunction function : windowFunctions) {
            Range range = new Range(-1, -1);
            function.processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    range.getStart(),
                    range.getEnd());
            channel++;
        }
    }

    // the output for empty match refers to empty pattern match and empty frame.
    private void outputEmptyMatch(PageBuilder pageBuilder)
    {
        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // compute measures
        for (MeasureComputation measureComputation : measures) {
            Block result = measureComputation.computeEmpty(matchNumber);
            measureComputation.getType().appendTo(result, 0, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // window functions have empty frame
        for (WindowFunction function : windowFunctions) {
            Range range = new Range(-1, -1);
            function.processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    range.getStart(),
                    range.getEnd());
            channel++;
        }
    }

    private void outputOneRowPerMatch(PageBuilder pageBuilder, MatchResult matchResult, int patternStart, int searchStart, int searchEnd)
    {
        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // compute measures from the position of the last row of the match
        ArrayView labels = matchResult.getLabels();
        for (MeasureComputation measureComputation : measures) {
            Block result = measureComputation.compute(patternStart + labels.length() - 1, labels, partitionStart, searchStart, searchEnd, patternStart, matchNumber, windowIndex);
            measureComputation.getType().appendTo(result, 0, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // window functions have frame consisting of all rows of the match
        for (WindowFunction function : windowFunctions) {
            function.processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    patternStart - partitionStart,
                    patternStart + labels.length() - 1 - partitionStart);
            channel++;
        }
    }

    private void outputAllRowsPerMatch(PageBuilder pageBuilder, MatchResult matchResult, int searchStart, int searchEnd)
    {
        // window functions are not allowed with ALL ROWS PER MATCH
        checkState(windowFunctions.isEmpty(), "invalid node: window functions specified with ALL ROWS PER MATCH");

        ArrayView labels = matchResult.getLabels();
        ArrayView exclusions = matchResult.getExclusions();

        int start = 0;
        for (int index = 0; index < exclusions.length(); index += 2) {
            int end = exclusions.get(index);

            for (int i = start; i < end; i++) {
                outputRow(pageBuilder, labels, currentPosition + i, searchStart, searchEnd);
            }

            start = exclusions.get(index + 1);
        }

        for (int i = start; i < labels.length(); i++) {
            outputRow(pageBuilder, labels, currentPosition + i, searchStart, searchEnd);
        }
    }

    private void outputRow(PageBuilder pageBuilder, ArrayView labels, int position, int searchStart, int searchEnd)
    {
        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], position, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
        // compute measures from the current position (the position from which measures are computed matters in RUNNING semantics)
        for (MeasureComputation measureComputation : measures) {
            Block result = measureComputation.compute(position, labels, partitionStart, searchStart, searchEnd, currentPosition, matchNumber, windowIndex);
            measureComputation.getType().appendTo(result, 0, pageBuilder.getBlockBuilder(channel));
            channel++;
        }
    }

    private void updateLastMatchedPosition(MatchResult matchResult, int patternStart)
    {
        int lastPositionInMatch = patternStart + matchResult.getLabels().length() - 1;
        lastMatchedPosition = max(lastMatchedPosition, lastPositionInMatch);
    }

    private void skipAfterMatch(MatchResult matchResult, int patternStart, int searchStart, int searchEnd)
    {
        ArrayView labels = matchResult.getLabels();
        switch (skipToPosition) {
            case PAST_LAST:
                lastSkippedPosition = patternStart + labels.length() - 1;
                break;
            case NEXT:
                lastSkippedPosition = currentPosition;
                break;
            case LAST:
            case FIRST:
                checkState(skipToNavigation.isPresent(), "skip to navigation is missing for SKIP TO ", skipToPosition.name());
                int position = skipToNavigation.get().resolvePosition(patternStart + labels.length() - 1, labels, searchStart, searchEnd, patternStart);
                if (position == -1) {
                    throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, "AFTER MATCH SKIP failed: pattern variable is not present in match");
                }
                if (position == patternStart) {
                    throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, "AFTER MATCH SKIP failed: cannot skip to first row of match");
                }
                lastSkippedPosition = position - 1;
                break;
            default:
                throw new IllegalStateException("unexpected SKIP TO position: " + skipToPosition);
        }
    }

    private void updatePeerGroup()
    {
        peerGroupStart = currentPosition;
        // find end of peer group
        peerGroupEnd = peerGroupStart + 1;
        while ((peerGroupEnd < partitionEnd) && pagesIndex.positionNotDistinctFromPosition(peerGroupHashStrategy, peerGroupStart, peerGroupEnd)) {
            peerGroupEnd++;
        }
    }
}
