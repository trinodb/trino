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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.block.BlockAssertions;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.operator.PageAssertions;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class BufferTestUtils
{
    private BufferTestUtils() {}

    private static final PagesSerdeFactory PAGES_SERDE_FACTORY = new TestingPagesSerdeFactory();
    static final Duration NO_WAIT = new Duration(0, MILLISECONDS);
    static final Duration MAX_WAIT = new Duration(1, SECONDS);
    private static final DataSize BUFFERED_PAGE_SIZE = DataSize.ofBytes(serializePage(createPage(42)).getRetainedSize());

    static BufferResult getFuture(ListenableFuture<BufferResult> future, Duration maxWait)
    {
        Optional<BufferResult> bufferResult = tryGetFutureValue(future, (int) maxWait.toMillis(), MILLISECONDS);
        checkArgument(bufferResult.isPresent(), "bufferResult is empty");
        return bufferResult.get();
    }

    static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getSerializedPages().size(), expected.getSerializedPages().size(), "page count");
        assertEquals(actual.getToken(), expected.getToken(), "token");
        PageDeserializer deserializer = PAGES_SERDE_FACTORY.createDeserializer(Optional.empty());
        for (int i = 0; i < actual.getSerializedPages().size(); i++) {
            Page actualPage = deserializer.deserialize(actual.getSerializedPages().get(i));
            Page expectedPage = deserializer.deserialize(expected.getSerializedPages().get(i));
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferComplete(), expected.isBufferComplete(), "buffer complete");
    }

    static BufferResult createBufferResult(String bufferId, long token, List<Page> pages)
    {
        checkArgument(!pages.isEmpty(), "pages is empty");
        ImmutableList.Builder<Slice> builder = ImmutableList.builderWithExpectedSize(pages.size());
        PageSerializer serializer = PAGES_SERDE_FACTORY.createSerializer(Optional.empty());
        for (Page p : pages) {
            builder.add(serializer.serialize(p));
        }
        return new BufferResult(
                bufferId,
                token,
                token + pages.size(),
                false,
                builder.build());
    }

    public static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    static Slice serializePage(Page page)
    {
        return PAGES_SERDE_FACTORY.createSerializer(Optional.empty()).serialize(page);
    }

    static DataSize sizeOfPages(int count)
    {
        return DataSize.ofBytes(BUFFERED_PAGE_SIZE.toBytes() * count);
    }

    static BufferResult getBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    // TODO: remove this after PR is landed: https://github.com/prestodb/presto/pull/7987
    static void acknowledgeBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId)
    {
        buffer.acknowledge(bufferId, sequenceId);
    }

    static ListenableFuture<Void> enqueuePage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(ImmutableList.of(serializePage(page)));
        ListenableFuture<Void> future = buffer.isFull();
        assertFalse(future.isDone());
        return future;
    }

    static ListenableFuture<Void> enqueuePage(OutputBuffer buffer, Page page, int partition)
    {
        buffer.enqueue(partition, ImmutableList.of(serializePage(page)));
        ListenableFuture<Void> future = buffer.isFull();
        assertFalse(future.isDone());
        return future;
    }

    public static void addPage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(ImmutableList.of(serializePage(page)));
        assertTrue(buffer.isFull().isDone(), "Expected add page to not block");
    }

    public static void addPage(OutputBuffer buffer, Page page, int partition)
    {
        buffer.enqueue(partition, ImmutableList.of(serializePage(page)));
        assertTrue(buffer.isFull().isDone(), "Expected add page to not block");
    }

    static void assertQueueState(
            OutputBuffer buffer,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        assertEquals(
                getBufferInfo(buffer, bufferId),
                new PipelinedBufferInfo(
                        bufferId,
                        // every page has one row
                        bufferedPages + pagesSent,
                        bufferedPages + pagesSent,
                        bufferedPages,
                        sizeOfPages(bufferedPages).toBytes(),
                        pagesSent,
                        false));
    }

    static void assertQueueState(
            OutputBuffer buffer,
            int unassignedPages,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream().mapToInt(PipelinedBufferInfo::getBufferedPages).sum();

        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        PipelinedBufferInfo bufferInfo = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(
                bufferInfo,
                new PipelinedBufferInfo(
                        bufferId,
                        // every page has one row
                        bufferedPages + pagesSent,
                        bufferedPages + pagesSent,
                        bufferedPages,
                        sizeOfPages(bufferedPages).toBytes(),
                        pagesSent,
                        false));
    }

    @SuppressWarnings("ConstantConditions")
    static void assertQueueClosed(OutputBuffer buffer, OutputBufferId bufferId, int pagesSent)
    {
        PipelinedBufferInfo bufferInfo = getBufferInfo(buffer, bufferId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    @SuppressWarnings("ConstantConditions")
    static void assertQueueClosed(OutputBuffer buffer, int unassignedPages, OutputBufferId bufferId, int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream().mapToInt(PipelinedBufferInfo::getBufferedPages).sum();
        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        PipelinedBufferInfo bufferInfo = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    static void assertFinished(OutputBuffer buffer)
    {
        assertEquals(buffer.getState(), FINISHED);
        for (PipelinedBufferInfo bufferInfo : buffer.getInfo().getPipelinedBufferStates().orElse(ImmutableList.of())) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    static void assertFutureIsDone(Future<?> future)
    {
        tryGetFutureValue(future, 5, SECONDS);
        assertTrue(future.isDone());
    }

    private static PipelinedBufferInfo getBufferInfo(OutputBuffer buffer, OutputBufferId bufferId)
    {
        for (PipelinedBufferInfo bufferInfo : buffer.getInfo().getPipelinedBufferStates().orElse(ImmutableList.of())) {
            if (bufferInfo.getBufferId().equals(bufferId)) {
                return bufferInfo;
            }
        }
        return null;
    }
}
