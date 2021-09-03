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
package io.trino.operator.window.pattern;

import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.operator.project.PageProjection;
import io.trino.operator.window.matcher.ArrayView;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class LabelEvaluator
{
    private final long matchNumber;

    private final int patternStart;

    // inclusive - the first row of the search partition
    private final int partitionStart;

    // inclusive - the first row of the partition area available for pattern search.
    // this area is the whole partition in case of MATCH_RECOGNIZE, and the area enclosed
    // by the common base frame in case of pattern recognition in WINDOW clause.
    private final int searchStart;

    // exclusive - the first row after the the partition area available for pattern search.
    // this area is the whole partition in case of MATCH_RECOGNIZE, and the area enclosed
    // by the common base frame in case of pattern recognition in WINDOW clause.
    private final int searchEnd;

    private final List<Evaluation> evaluations;

    private final WindowIndex windowIndex;

    public LabelEvaluator(long matchNumber, int patternStart, int partitionStart, int searchStart, int searchEnd, List<Evaluation> evaluations, WindowIndex windowIndex)
    {
        this.matchNumber = matchNumber;
        this.patternStart = patternStart;
        this.partitionStart = partitionStart;
        this.searchStart = searchStart;
        this.searchEnd = searchEnd;
        this.evaluations = requireNonNull(evaluations, "evaluations is null");
        this.windowIndex = requireNonNull(windowIndex, "windowIndex is null");
    }

    public int getInputLength()
    {
        return searchEnd - patternStart;
    }

    public boolean isMatchingAtPartitionStart()
    {
        return patternStart == partitionStart;
    }

    public boolean evaluateLabel(int label, ArrayView matchedLabels)
    {
        Evaluation evaluation = evaluations.get(label);
        return evaluation.test(label, matchedLabels, partitionStart, searchStart, searchEnd, patternStart, matchNumber, windowIndex);
    }

    public static class Evaluation
    {
        // compiled computation of label-defining boolean expression
        private final PageProjection projection;

        // value accessors ordered as expected by the compiled projection
        private final List<PhysicalValuePointer> expectedLayout;

        // mapping from int representation to label name
        private final List<String> labelNames;

        private final ConnectorSession session;

        public Evaluation(PageProjection projection, List<PhysicalValuePointer> expectedLayout, List<String> labelNames, ConnectorSession session)
        {
            this.projection = requireNonNull(projection, "projection is null");
            this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
            this.labelNames = requireNonNull(labelNames, "labelNames is null");
            this.session = requireNonNull(session, "session is null");
        }

        public List<PhysicalValuePointer> getExpectedLayout()
        {
            return expectedLayout;
        }

        // TODO This method allocates an intermediate block and passes it as the input to the pre-compiled expression.
        //  Instead, the expression should be compiled directly against the row navigations.
        //  The same applies to MeasureComputation.compute()
        public boolean test(int label, ArrayView matchedLabels, int partitionStart, int searchStart, int searchEnd, int patternStart, long matchNumber, WindowIndex windowIndex)
        {
            // get values at appropriate positions and prepare input for the projection as an array of single-value blocks
            Block[] blocks = new Block[expectedLayout.size()];
            for (int i = 0; i < expectedLayout.size(); i++) {
                PhysicalValuePointer pointer = expectedLayout.get(i);
                int channel = pointer.getSourceChannel();
                if (channel == MATCH_NUMBER) {
                    blocks[i] = nativeValueToBlock(BIGINT, matchNumber);
                }
                else {
                    int position = pointer.getLogicalIndexNavigation().resolvePosition(matchedLabels, label, searchStart, searchEnd, patternStart);
                    if (position >= 0) {
                        if (channel == CLASSIFIER) {
                            Type type = VARCHAR;
                            if (position < patternStart || position > patternStart + matchedLabels.length()) {
                                // position out of match. classifier() function returns null.
                                blocks[i] = nativeValueToBlock(type, null);
                            }
                            else if (position == patternStart + matchedLabels.length()) {
                                // currently matched position is considered to have the label temporarily assigned for the purpose of match evaluation. the label is not yet present in matchedLabels.
                                blocks[i] = nativeValueToBlock(type, utf8Slice(labelNames.get(label)));
                            }
                            else {
                                // position already matched. get the assigned label from matchedLabels.
                                blocks[i] = nativeValueToBlock(type, utf8Slice(labelNames.get(matchedLabels.get(position - patternStart))));
                            }
                        }
                        else {
                            // TODO Block#getRegion
                            blocks[i] = windowIndex.getSingleValueBlock(channel, position - partitionStart);
                        }
                    }
                    else {
                        blocks[i] = nativeValueToBlock(pointer.getType(), null);
                    }
                }
            }

            // wrap block array into a single-row page
            Page page = new Page(1, blocks);

            // evaluate expression
            Work<Block> work = projection.project(session, new DriverYieldSignal(), projection.getInputChannels().getInputChannels(page), positionsRange(0, 1));
            boolean done = false;
            while (!done) {
                done = work.process();
            }
            Block result = work.getResult();
            return BOOLEAN.getBoolean(result, 0);
        }
    }

    public static class EvaluationSupplier
    {
        private final Supplier<PageProjection> projection;
        private final List<PhysicalValuePointer> expectedLayout;
        private final List<String> labelNames;
        private final ConnectorSession session;

        public EvaluationSupplier(Supplier<PageProjection> projection, List<PhysicalValuePointer> expectedLayout, List<String> labelNames, ConnectorSession session)
        {
            this.projection = requireNonNull(projection, "projection is null");
            this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
            this.labelNames = requireNonNull(labelNames, "labelNames is null");
            this.session = requireNonNull(session, "session is null");
        }

        public Evaluation get()
        {
            return new Evaluation(projection.get(), expectedLayout, labelNames, session);
        }
    }
}
