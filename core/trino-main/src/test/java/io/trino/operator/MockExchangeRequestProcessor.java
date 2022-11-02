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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PagesSerde;
import io.trino.server.InternalHeaders;
import io.trino.spi.Page;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.trino.TrinoMediaTypes.TRINO_PAGES;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static io.trino.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static io.trino.server.InternalHeaders.TRINO_BUFFER_COMPLETE;
import static io.trino.server.InternalHeaders.TRINO_PAGE_NEXT_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_PAGE_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_TASK_FAILED;
import static io.trino.server.InternalHeaders.TRINO_TASK_INSTANCE_ID;
import static io.trino.server.PagesResponseWriter.SERIALIZED_PAGES_MAGIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MockExchangeRequestProcessor
        implements TestingHttpClient.Processor
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";
    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    private final LoadingCache<URI, MockBuffer> buffers = buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(MockBuffer::new));

    private final DataSize expectedMaxSize;

    public MockExchangeRequestProcessor(DataSize expectedMaxSize)
    {
        this.expectedMaxSize = expectedMaxSize;
    }

    public void addPage(URI location, Page page)
    {
        buffers.getUnchecked(location).addPage(page);
    }

    public void addPage(URI location, Slice page)
    {
        buffers.getUnchecked(location).addPage(page);
    }

    public void setComplete(URI location)
    {
        buffers.getUnchecked(location).setCompleted();
    }

    public void setFailed(URI location, RuntimeException failure)
    {
        buffers.getUnchecked(location).setFailed(failure);
    }

    @Override
    public Response handle(Request request)
    {
        if (request.getMethod().equalsIgnoreCase("DELETE")) {
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        }

        // verify we got a data size and it parses correctly
        assertTrue(!request.getHeaders().get(InternalHeaders.TRINO_MAX_SIZE).isEmpty());
        DataSize maxSize = DataSize.valueOf(request.getHeader(InternalHeaders.TRINO_MAX_SIZE));
        assertEquals(maxSize, expectedMaxSize);

        RequestLocation requestLocation = new RequestLocation(request.getUri());
        URI location = requestLocation.getLocation();

        BufferResult result = buffers.getUnchecked(location).getPages(requestLocation.getSequenceId(), maxSize);

        byte[] bytes = new byte[0];
        HttpStatus status;
        if (!result.getSerializedPages().isEmpty()) {
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(64);
            sliceOutput.writeInt(SERIALIZED_PAGES_MAGIC);
            sliceOutput.writeLong(calculateChecksum(result.getSerializedPages()));
            sliceOutput.writeInt(result.getSerializedPages().size());
            for (Slice page : result.getSerializedPages()) {
                sliceOutput.writeBytes(page);
            }
            bytes = sliceOutput.slice().getBytes();
            status = HttpStatus.OK;
        }
        else {
            status = HttpStatus.NO_CONTENT;
        }

        return new TestingResponse(
                status,
                ImmutableListMultimap.<String, String>builder()
                        .put(CONTENT_TYPE, TRINO_PAGES)
                        .put(TRINO_TASK_INSTANCE_ID, String.valueOf(result.getTaskInstanceId()))
                        .put(TRINO_PAGE_TOKEN, String.valueOf(result.getToken()))
                        .put(TRINO_PAGE_NEXT_TOKEN, String.valueOf(result.getNextToken()))
                        .put(TRINO_BUFFER_COMPLETE, String.valueOf(result.isBufferComplete()))
                        .put(TRINO_TASK_FAILED, "false")
                        .build(),
                bytes);
    }

    private static class RequestLocation
    {
        private final URI location;
        private final long sequenceId;

        public RequestLocation(URI uri)
        {
            String string = uri.toString();
            int index = string.lastIndexOf('/');
            location = URI.create(string.substring(0, index));
            sequenceId = Long.parseLong(string.substring(index + 1));
        }

        public URI getLocation()
        {
            return location;
        }

        public long getSequenceId()
        {
            return sequenceId;
        }
    }

    private static class MockBuffer
    {
        private final URI location;
        private final AtomicBoolean completed = new AtomicBoolean();
        private final AtomicLong token = new AtomicLong();
        private final BlockingQueue<Slice> serializedPages = new LinkedBlockingQueue<>();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

        private MockBuffer(URI location)
        {
            this.location = location;
        }

        public void setCompleted()
        {
            completed.set(true);
        }

        public synchronized void addPage(Slice page)
        {
            checkState(completed.get() != Boolean.TRUE, "Location %s is complete", location);
            serializedPages.add(page);
        }

        public synchronized void addPage(Page page)
        {
            checkState(completed.get() != Boolean.TRUE, "Location %s is complete", location);
            try (PagesSerde.PagesSerdeContext context = PAGES_SERDE.newContext()) {
                serializedPages.add(PAGES_SERDE.serialize(context, page));
            }
        }

        public void setFailed(RuntimeException t)
        {
            failure.set(t);
        }

        public BufferResult getPages(long sequenceId, DataSize maxSize)
        {
            // if location is complete return GONE
            if (completed.get() && serializedPages.isEmpty()) {
                return BufferResult.emptyResults(TASK_INSTANCE_ID, token.get(), true);
            }

            RuntimeException failure = this.failure.get();
            if (failure != null) {
                throw failure;
            }

            assertEquals(sequenceId, token.get(), "token");

            // wait for a single page to arrive
            Slice serializedPage = null;
            try {
                serializedPage = serializedPages.poll(10, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // if no page, return NO CONTENT
            if (serializedPage == null) {
                return BufferResult.emptyResults(TASK_INSTANCE_ID, token.get(), false);
            }

            // add serializedPages up to the size limit
            List<Slice> responsePages = new ArrayList<>();
            responsePages.add(serializedPage);
            long responseSize = serializedPage.length();
            while (responseSize < maxSize.toBytes()) {
                serializedPage = serializedPages.poll();
                if (serializedPage == null) {
                    break;
                }
                responsePages.add(serializedPage);
                responseSize += serializedPage.length();
            }

            // update sequence id
            long nextToken = token.get() + responsePages.size();

            BufferResult bufferResult = new BufferResult(TASK_INSTANCE_ID, token.get(), nextToken, false, responsePages);
            token.set(nextToken);

            return bufferResult;
        }
    }
}
