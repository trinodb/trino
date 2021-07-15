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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class MeasureComputation
{
    // compiled computation of expression
    private final PageProjection projection;

    // value accessors ordered as expected by the compiled projection
    private final List<PhysicalValuePointer> expectedLayout;

    // result type
    private final Type type;

    // mapping from int representation to label name
    private final List<String> labelNames;

    private final ConnectorSession session;

    public MeasureComputation(PageProjection projection, List<PhysicalValuePointer> expectedLayout, Type type, List<String> labelNames, ConnectorSession session)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
        this.type = requireNonNull(type, "type is null");
        this.labelNames = requireNonNull(labelNames, "labelNames is null");
        this.session = requireNonNull(session, "session is null");
    }

    public Type getType()
    {
        return type;
    }

    // TODO This method allocates an intermediate block and passes it as the input to the pre-compiled expression.
    //  Instead, the expression should be compiled directly against the row navigations.
    //  The same applies to LabelEvaluator.Evaluation.test()
    public Block compute(int currentRow, ArrayView matchedLabels, int partitionStart, int searchStart, int searchEnd, int patternStart, long matchNumber, WindowIndex windowIndex)
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
                int position = pointer.getLogicalIndexNavigation().resolvePosition(currentRow, matchedLabels, searchStart, searchEnd, patternStart);
                if (position >= 0) {
                    if (channel == CLASSIFIER) {
                        Type type = VARCHAR;
                        if (position < patternStart || position >= patternStart + matchedLabels.length()) {
                            // position out of match. classifier() function returns null.
                            blocks[i] = nativeValueToBlock(type, null);
                        }
                        else {
                            // position within match. get the assigned label from matchedLabels.
                            // note: when computing measures, all labels of the match can be accessed (even those exceeding the current running position), both in RUNNING and FINAL semantics
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
        return work.getResult();
    }

    public Block computeEmpty(long matchNumber)
    {
        // prepare input for the projection as an array of single-value blocks. for empty match:
        // - match_number() is the sequential number of the match
        // - classifier() is null
        // - all value references are null
        Block[] blocks = new Block[expectedLayout.size()];
        for (int i = 0; i < expectedLayout.size(); i++) {
            PhysicalValuePointer physicalValuePointer = expectedLayout.get(i);
            if (physicalValuePointer.getSourceChannel() == MATCH_NUMBER) {
                blocks[i] = nativeValueToBlock(BIGINT, matchNumber);
            }
            else {
                blocks[i] = nativeValueToBlock(physicalValuePointer.getType(), null);
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
        return work.getResult();
    }

    public static class MeasureComputationSupplier
    {
        private final Supplier<PageProjection> projection;
        private final List<PhysicalValuePointer> expectedLayout;
        private final Type type;
        private final List<String> labelNames;
        private final ConnectorSession session;

        public MeasureComputationSupplier(Supplier<PageProjection> projection, List<PhysicalValuePointer> expectedLayout, Type type, List<String> labelNames, ConnectorSession session)
        {
            this.projection = requireNonNull(projection, "projection is null");
            this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
            this.type = requireNonNull(type, "type is null");
            this.labelNames = requireNonNull(labelNames, "labelNames is null");
            this.session = requireNonNull(session, "session is null");
        }

        public MeasureComputation get()
        {
            return new MeasureComputation(projection.get(), expectedLayout, type, labelNames, session);
        }
    }
}
