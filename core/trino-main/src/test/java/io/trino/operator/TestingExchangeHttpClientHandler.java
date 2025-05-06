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

import com.google.common.base.Splitter;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.spi.Page;

import java.util.List;
import java.util.Optional;

import static io.trino.TrinoMediaTypes.TRINO_PAGES;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static io.trino.server.InternalHeaders.TRINO_BUFFER_COMPLETE;
import static io.trino.server.InternalHeaders.TRINO_PAGE_NEXT_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_PAGE_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_TASK_FAILED;
import static io.trino.server.InternalHeaders.TRINO_TASK_INSTANCE_ID;
import static io.trino.server.PagesInputStreamFactory.SERIALIZED_PAGES_MAGIC;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestingExchangeHttpClientHandler
        implements TestingHttpClient.Processor
{
    private final LoadingCache<TaskId, TestingTaskBuffer> taskBuffers;
    private final PagesSerdeFactory serdeFactory;

    public TestingExchangeHttpClientHandler(LoadingCache<TaskId, TestingTaskBuffer> taskBuffers, PagesSerdeFactory serdeFactory)
    {
        this.taskBuffers = requireNonNull(taskBuffers, "taskBuffers is null");
        this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
    }

    @Override
    public Response handle(Request request)
    {
        List<String> parts = ImmutableList.copyOf(Splitter.on("/").omitEmptyStrings().split(request.getUri().getPath()));
        if (request.getMethod().equals("DELETE")) {
            assertThat(parts).hasSize(1);
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        }

        assertThat(parts).hasSize(2);
        TaskId taskId = TaskId.valueOf(parts.get(0));
        int pageToken = Integer.parseInt(parts.get(1));

        ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.builder();
        headers.put(TRINO_TASK_INSTANCE_ID, "task-instance-id");
        headers.put(TRINO_PAGE_TOKEN, String.valueOf(pageToken));
        headers.put(TRINO_TASK_FAILED, "false");

        TestingTaskBuffer taskBuffer = taskBuffers.getUnchecked(taskId);
        Page page = taskBuffer.getPage(pageToken);
        headers.put(CONTENT_TYPE, TRINO_PAGES);
        if (page != null) {
            headers.put(TRINO_PAGE_NEXT_TOKEN, String.valueOf(pageToken + 1));
            headers.put(TRINO_BUFFER_COMPLETE, String.valueOf(false));
            Slice serializedPage = serdeFactory.createSerializer(Optional.empty()).serialize(page);
            DynamicSliceOutput output = new DynamicSliceOutput(256);
            output.writeInt(SERIALIZED_PAGES_MAGIC);
            output.writeLong(calculateChecksum(ImmutableList.of(serializedPage)));
            output.writeInt(1);
            output.writeBytes(serializedPage);
            return new TestingResponse(HttpStatus.OK, headers.build(), output.slice().getInput());
        }
        if (taskBuffer.isFinished()) {
            headers.put(TRINO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
            headers.put(TRINO_BUFFER_COMPLETE, String.valueOf(true));
            DynamicSliceOutput output = new DynamicSliceOutput(8);
            output.writeInt(SERIALIZED_PAGES_MAGIC);
            output.writeLong(calculateChecksum(ImmutableList.of()));
            output.writeInt(0);
            return new TestingResponse(HttpStatus.OK, headers.build(), output.slice().getInput());
        }
        headers.put(TRINO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
        headers.put(TRINO_BUFFER_COMPLETE, String.valueOf(false));
        return new TestingResponse(HttpStatus.NO_CONTENT, headers.build(), new byte[0]);
    }
}
