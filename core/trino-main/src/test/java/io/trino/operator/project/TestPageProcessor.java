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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.TestingSourcePage;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPageProcessor
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testProjectNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), Optional.empty(), ImmutableList.of(), OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        Page outputPage = getOnlyElement(outputPages).orElseThrow();
        assertThat(outputPage.getChannelCount()).isEqualTo(0);
        assertThat(outputPage.getPositionCount()).isEqualTo(inputPage.getPositionCount());
    }

    @Test
    public void testFilterNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new TestingPageFilter(positionsRange(0, 50)))), ImmutableList.of());

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        Page outputPage = getOnlyElement(outputPages).orElseThrow();
        assertThat(outputPage.getChannelCount()).isEqualTo(0);
        assertThat(outputPage.getPositionCount()).isEqualTo(50);
    }

    @Test
    public void testPartialFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new TestingPageFilter(positionsRange(25, 50)))),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        Page outputPage = getOnlyElement(outputPages).orElseThrow();
        assertPageEquals(ImmutableList.of(BIGINT), outputPage, new Page(createLongSequenceBlock(25, 75)));
    }

    @Test
    public void testSelectAllFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        Page outputPage = getOnlyElement(outputPages).orElseThrow();
        assertPageEquals(ImmutableList.of(BIGINT), outputPage, new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testSelectNoneFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectNoneFilter())), ImmutableList.of(new InputPageProjection(0)));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testProjectEmptyPage()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectAllFilter())), ImmutableList.of(new InputPageProjection(0)));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 0));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        // output should be one page containing no columns (only a count)
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testSelectNoneFilterLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectNoneFilter())), ImmutableList.of(new InputPageProjection(1)));

        // if channel 1 is loaded, test will fail
        SourcePage inputPage = new TestingSourcePage(100, createLongSequenceBlock(0, 100), null);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testProjectLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new LazyPagePageProjection()),
                OptionalInt.of(MAX_BATCH_SIZE));

        // if channel 1 is loaded, test will fail
        SourcePage inputPage = new TestingSourcePage(100, createLongSequenceBlock(0, 100), null);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, memoryContext, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        Page outputPage = getOnlyElement(outputPages).orElseThrow();
        assertPageEquals(ImmutableList.of(BIGINT), outputPage, new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testBatchedOutput()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, (int) (MAX_BATCH_SIZE * 2.5)));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(3);
        for (int i = 0; i < outputPages.size(); i++) {
            Page actualPage = outputPages.get(i).orElse(null);
            int offset = i * MAX_BATCH_SIZE;
            Page expectedPage = new Page(createLongSequenceBlock(offset, offset + Math.min(inputPage.getPositionCount() - offset, MAX_BATCH_SIZE)));
            assertPageEquals(ImmutableList.of(BIGINT), actualPage, expectedPage);
        }
    }

    @Test
    public void testAdaptiveBatchSize()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new TestingPassthroughProjection(0)),
                OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(4096));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        int batchSize = MAX_BATCH_SIZE;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize)));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }

        // process small page which will increase batch size
        Arrays.fill(slices, Slices.allocate(128));
        inputPage = SourcePage.create(createSlicesBlock(slices));

        output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        outputPages = ImmutableList.copyOf(output);
        int offset = 0;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, Math.min(inputPage.getPositionCount() - offset, batchSize))));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            offset += actualPage.orElseThrow(() -> new AssertionError("page is not present")).getPositionCount();
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() < MIN_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize * 2;
            }
        }
    }

    @Test
    public void testOptimisticProcessing()
    {
        InvocationCountPageProjection firstProjection = new InvocationCountPageProjection(new InputPageProjection(0));
        InvocationCountPageProjection secondProjection = new InvocationCountPageProjection(new InputPageProjection(0));
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(firstProjection, secondProjection),
                OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(4096));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        // batch size will be reduced before the first page is produced until the first block is within the page size bounds
        int batchSize = MAX_BATCH_SIZE;
        while (inputPage.getBlock(0).getRegionSizeInBytes(0, batchSize) > MAX_PAGE_SIZE_IN_BYTES) {
            batchSize /= 2;
        }

        int pageCount = 0;
        while (output.hasNext()) {
            Page actualPage = output.next().orElse(null);
            Block sliceBlock = createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize));
            Page expectedPage = new Page(sliceBlock, sliceBlock);
            assertPageEquals(ImmutableList.of(VARCHAR, VARCHAR), actualPage, expectedPage);
            pageCount++;

            // batch size will be further reduced to fit within the bounds
            if (actualPage.getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }
        // second project is invoked once per output page
        assertThat(secondProjection.getInvocationCount()).isEqualTo(pageCount);

        // the page processor saves the results when the page size is exceeded, so the first projection
        // will be invoked less times
        assertThat(firstProjection.getInvocationCount() < secondProjection.getInvocationCount()).isTrue();
    }

    @Test
    public void testRetainedSize()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new TestingPassthroughProjection(0), new TestingPassthroughProjection(1)),
                OptionalInt.of(100));

        // create 2 columns X 800 rows of strings with each string's size = 30KB
        // the page is produced in multiple batches and retained between them
        String value = join("", nCopies(30_000, "a"));
        List<String> values = nCopies(800, value);
        SourcePage inputPage = SourcePage.create(new Page(createStringsBlock(values), createStringsBlock(values)));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, memoryContext, inputPage);

        // force a compute
        assertThat(output.hasNext()).isTrue();

        // verify we do not count block sizes twice
        assertThat(memoryContext.getBytes()).isCloseTo(inputPage.getRetainedSizeInBytes(), Offset.offset(200L));
    }

    @Test
    public void testIncreasingBatchSize()
    {
        int rows = 1024;

        // small output pages keep doubling the batch size until other limits are hit
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new TestingPassthroughProjection(0)),
                OptionalInt.of(1));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        long previousPositionCount = 1;
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertThat(page).isPresent();
            long positionCount = page.get().getPositionCount();
            totalPositionCount += positionCount;
            // skip the first read && skip the last read, which can be a partial page
            if (positionCount > 1 && totalPositionCount != rows) {
                assertThat(positionCount).isEqualTo(previousPositionCount * 2);
            }
            previousPositionCount = positionCount;
        }
    }

    @Test
    public void testDecreasingBatchSize()
    {
        int rows = 1024;

        // output pages over MAX_PAGE_SIZE_IN_BYTES halve the batch size until output fits
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new TestingPassthroughProjection(0)),
                OptionalInt.of(512));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(64 * 1024));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Integer> batchSizes = new ArrayList<>();
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertThat(page).isPresent();
            batchSizes.add(page.get().getPositionCount());
            totalPositionCount += page.get().getPositionCount();
        }
        // 512 and 256 positions of 64KB slices exceed 16MB, 128 positions fit
        assertThat(batchSizes).isEqualTo(ImmutableList.of(512, 256, 128, 128));
    }

    @Test
    public void testIdentityProjectionsAreNotBatched()
    {
        int rows = 1024;

        // identity projections produce the whole selection in one batch regardless of output size
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0)),
                OptionalInt.of(1));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(64 * 1024));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        Optional<Page> page = output.next();
        assertThat(page).isPresent();
        assertThat(page.get().getPositionCount()).isEqualTo(rows);
        assertThat(output.hasNext()).isFalse();
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, SourcePage inputPage)
    {
        return processAndAssertRetainedPageSize(pageProcessor, newSimpleAggregatedMemoryContext(), inputPage);
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, AggregatedMemoryContext memoryContext, SourcePage inputPage)
    {
        Iterator<Optional<Page>> output = pageProcessor.process(
                SESSION,
                memoryContext.newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        return output;
    }

    private static class InvocationCountPageProjection
            implements PageProjection
    {
        protected final PageProjection delegate;
        private int invocationCount;

        public InvocationCountPageProjection(PageProjection delegate)
        {
            this.delegate = delegate;
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
            setInvocationCount(getInvocationCount() + 1);
            return delegate.project(session, page, selectedPositions);
        }

        public int getInvocationCount()
        {
            return invocationCount;
        }

        public void setInvocationCount(int invocationCount)
        {
            this.invocationCount = invocationCount;
        }
    }

    private static class TestingPassthroughProjection
            implements PageProjection
    {
        private final int channel;

        private TestingPassthroughProjection(int channel)
        {
            this.channel = channel;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(channel);
        }

        @Override
        public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
        {
            Block block = page.getBlock(0);
            if (selectedPositions.isList()) {
                return block.getPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
            }
            return block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
    }

    public static class LazyPagePageProjection
            implements PageProjection
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0, 1);
        }

        @Override
        public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
        {
            Block block = page.getBlock(0);
            if (selectedPositions.isList()) {
                return block.getPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
            }
            return block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
    }

    private static class TestingPageFilter
            implements PageFilter
    {
        private final SelectedPositions selectedPositions;

        public TestingPageFilter(SelectedPositions selectedPositions)
        {
            this.selectedPositions = selectedPositions;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return selectedPositions;
        }
    }

    public static class SelectAllFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return positionsRange(0, page.getPositionCount());
        }
    }

    private static class SelectNoneFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return positionsRange(0, 0);
        }
    }
}
