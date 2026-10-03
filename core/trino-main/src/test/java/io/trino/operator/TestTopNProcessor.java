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

import com.google.common.collect.ImmutableList;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.InputPageProjection;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.PageProjectionsProcessor;
import io.trino.operator.project.SelectedPositions;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.TopNProcessor.SKIPPED_DECODE_POSITIONS;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTopNProcessor
{
    @Test
    public void testMaskedInputProjectsPayloadOnlyForCandidates()
    {
        TopNProcessor processor = new TopNProcessor(
                newSimpleAggregatedMemoryContext(),
                ImmutableList.of(BIGINT, BIGINT),
                2,
                ImmutableList.of(0),
                new SimplePageWithPositionComparator(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_LAST), new TypeOperators()));
        CountingPageProjection payloadProjection = new CountingPageProjection(new InputPageProjection(1));
        PageProjectionsProcessor projectionsProcessor = new PageProjectionsProcessor(
                ImmutableList.of(new InputPageProjection(0), payloadProjection),
                MAX_BATCH_SIZE);

        addMaskedInput(processor, projectionsProcessor, createLongsBlock(5, 3, 8, 1), createLongsBlock(50, 30, 80, 10));
        addMaskedInput(processor, projectionsProcessor, createLongsBlock(4, 0, 9, 2, 7), createLongsBlock(40, 0, 90, 20, 70));
        addMaskedInput(processor, projectionsProcessor, createLongsBlock(6, -1, -2, 9), createLongsBlock(60, -10, -20, 90));
        addMaskedInput(processor, projectionsProcessor, createLongsBlock(6, 7), createLongsBlock(60, 70));

        assertThat(payloadProjection.getProjectedPositions()).isEqualTo(8);
        assertThat(processor.getMetrics().getMetrics()).containsEntry(SKIPPED_DECODE_POSITIONS, new LongCount(7));
        Page output = processor.getOutput();
        assertBlockEquals(BIGINT, output.getBlock(0), createLongsBlock(-2, -1));
        assertBlockEquals(BIGINT, output.getBlock(1), createLongsBlock(-20, -10));
        assertThat(processor.getOutput()).isNull();
    }

    private static void addMaskedInput(TopNProcessor processor, PageProjectionsProcessor projectionsProcessor, Block sortBlock, Block payloadBlock)
    {
        Page page = new Page(sortBlock, payloadBlock);
        MaskedPage maskedPage = MaskedPage.applyMask(
                SESSION,
                SourcePage.create(page),
                positionsRange(0, page.getPositionCount()),
                projectionsProcessor,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"),
                new PageProcessorMetrics());
        processor.addMaskedInput(maskedPage);
        maskedPage.invalidate();
    }

    private static class CountingPageProjection
            implements PageProjection
    {
        private final PageProjection delegate;
        private long projectedPositions;

        private CountingPageProjection(PageProjection delegate)
        {
            this.delegate = delegate;
        }

        public long getProjectedPositions()
        {
            return projectedPositions;
        }

        @Override
        public boolean isDeterministic()
        {
            return delegate.isDeterministic();
        }

        @Override
        public InputChannels getInputChannels()
        {
            return delegate.getInputChannels();
        }

        @Override
        public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
        {
            projectedPositions += selectedPositions.size();
            return delegate.project(session, page, selectedPositions);
        }
    }
}
