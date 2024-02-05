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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.operator.WindowOperator.FrameBoundKey;
import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.pattern.ArgumentComputation;
import io.trino.operator.window.pattern.LabelEvaluator.Evaluation;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MatchAggregation;
import io.trino.operator.window.pattern.MeasureComputation;
import io.trino.spi.function.WindowFunction;
import io.trino.sql.planner.plan.RowsPerMatch;
import io.trino.sql.planner.plan.SkipToPosition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PatternRecognitionPartitioner
        implements Partitioner
{
    private final List<MeasureComputation> measures;
    private final List<MatchAggregation> measureAggregations;
    private final List<ArgumentComputation> measureComputationsAggregationArguments;
    private final Optional<FrameInfo> commonBaseFrame;
    private final RowsPerMatch rowsPerMatch;
    private final Optional<LogicalIndexNavigation> skipToNavigation;
    private final SkipToPosition skipToPosition;
    private final boolean initial;
    private final Matcher matcher;
    private final List<Evaluation> labelEvaluations;
    private final List<ArgumentComputation> labelEvaluationsAggregationArguments;
    private final List<String> labelNames;

    public PatternRecognitionPartitioner(
            List<MeasureComputation> measures,
            List<MatchAggregation> measureAggregations,
            List<ArgumentComputation> measureComputationsAggregationArguments,
            Optional<FrameInfo> commonBaseFrame,
            RowsPerMatch rowsPerMatch,
            Optional<LogicalIndexNavigation> skipToNavigation,
            SkipToPosition skipToPosition,
            boolean initial,
            Matcher matcher,
            List<Evaluation> labelEvaluations,
            List<ArgumentComputation> labelEvaluationsAggregationArguments,
            List<String> labelNames)
    {
        requireNonNull(measures, "measures is null");
        requireNonNull(measureAggregations, "measureAggregations is null");
        requireNonNull(measureComputationsAggregationArguments, "measureComputationsAggregationArguments is null");
        requireNonNull(commonBaseFrame, "commonBaseFrame is null");
        requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        requireNonNull(skipToNavigation, "skipToNavigation is null");
        requireNonNull(skipToPosition, "skipToPosition is null");
        requireNonNull(matcher, "matcher is null");
        requireNonNull(labelEvaluations, "labelEvaluations is null");
        requireNonNull(labelEvaluationsAggregationArguments, "labelEvaluationsAggregationArguments is null");
        requireNonNull(labelNames, "labelNames is null");

        this.measures = measures;
        this.measureAggregations = measureAggregations;
        this.measureComputationsAggregationArguments = measureComputationsAggregationArguments;
        this.commonBaseFrame = commonBaseFrame;
        this.rowsPerMatch = rowsPerMatch;
        this.skipToNavigation = skipToNavigation;
        this.skipToPosition = skipToPosition;
        this.initial = initial;
        this.matcher = matcher;
        this.labelEvaluations = labelEvaluations;
        this.labelEvaluationsAggregationArguments = labelEvaluationsAggregationArguments;
        this.labelNames = labelNames;
    }

    @Override
    public WindowPartition createPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            List<FrameInfo> frames,
            PagesHashStrategy peerGroupHashStrategy,
            Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators,
            AggregatedMemoryContext memoryContext)
    {
        return new PatternRecognitionPartition(
                pagesIndex,
                partitionStart,
                partitionEnd,
                outputChannels,
                windowFunctions,
                peerGroupHashStrategy,
                memoryContext,
                measures,
                measureAggregations,
                measureComputationsAggregationArguments,
                commonBaseFrame,
                rowsPerMatch,
                skipToNavigation,
                skipToPosition,
                initial,
                matcher,
                labelEvaluations,
                labelEvaluationsAggregationArguments,
                labelNames);
    }
}
