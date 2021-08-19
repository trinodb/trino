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

import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.pattern.LabelEvaluator.EvaluationSupplier;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MeasureComputation.MeasureComputationSupplier;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.SkipTo;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionPartitionerSupplier
        implements PartitionerSupplier
{
    private final List<MeasureComputationSupplier> measures;
    private final Optional<FrameInfo> commonBaseFrame;
    private final RowsPerMatch rowsPerMatch;
    private final Optional<LogicalIndexNavigation> skipToNavigation;
    private final SkipTo.Position skipToPosition;
    private final boolean initial;
    private final Matcher matcher;
    private final List<EvaluationSupplier> labelEvaluations;

    public PatternRecognitionPartitionerSupplier(
            List<MeasureComputationSupplier> measures,
            Optional<FrameInfo> commonBaseFrame,
            RowsPerMatch rowsPerMatch,
            Optional<LogicalIndexNavigation> skipToNavigation,
            SkipTo.Position skipToPosition,
            boolean initial,
            Matcher matcher,
            List<EvaluationSupplier> labelEvaluations)
    {
        requireNonNull(measures, "measures is null");
        requireNonNull(commonBaseFrame, "commonBaseFrame is null");
        requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        requireNonNull(skipToNavigation, "skipToNavigation is null");
        requireNonNull(skipToPosition, "skipToPosition is null");
        requireNonNull(matcher, "matcher is null");
        requireNonNull(labelEvaluations, "labelEvaluations is null");

        this.measures = measures;
        this.commonBaseFrame = commonBaseFrame;
        this.rowsPerMatch = rowsPerMatch;
        this.skipToNavigation = skipToNavigation;
        this.skipToPosition = skipToPosition;
        this.initial = initial;
        this.matcher = matcher;
        this.labelEvaluations = labelEvaluations;
    }

    @Override
    public Partitioner get()
    {
        return new PatternRecognitionPartitioner(
                measures.stream()
                        .map(MeasureComputationSupplier::get)
                        .collect(toImmutableList()),
                commonBaseFrame,
                rowsPerMatch,
                skipToNavigation,
                skipToPosition,
                initial,
                matcher,
                labelEvaluations.stream()
                        .map(EvaluationSupplier::get)
                        .collect(toImmutableList()));
    }
}
