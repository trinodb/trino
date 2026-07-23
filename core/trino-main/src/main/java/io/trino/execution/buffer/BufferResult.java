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
import io.airlift.slice.Slice;
import io.trino.spi.Page;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Pages read from a task output buffer. Pages are returned in serialized form, except
 * for a consumer running on the same node, which may receive raw pages by reference.
 * A single result carries pages of one kind only.
 */
public record BufferResult(
        long taskInstanceId,
        long token,
        long nextToken,
        boolean bufferComplete,
        List<Slice> serializedPages,
        List<Page> rawPages)
{
    public static BufferResult emptyResults(long taskInstanceId, long token, boolean bufferComplete)
    {
        return new BufferResult(taskInstanceId, token, token, bufferComplete, ImmutableList.of());
    }

    public BufferResult(long taskInstanceId, long token, long nextToken, boolean bufferComplete, List<Slice> serializedPages)
    {
        this(taskInstanceId, token, nextToken, bufferComplete, serializedPages, ImmutableList.of());
    }

    public BufferResult
    {
        serializedPages = ImmutableList.copyOf(requireNonNull(serializedPages, "serializedPages is null"));
        rawPages = ImmutableList.copyOf(requireNonNull(rawPages, "rawPages is null"));
        checkArgument(serializedPages.isEmpty() || rawPages.isEmpty(), "result cannot contain both serialized and raw pages");
    }

    public int size()
    {
        return serializedPages.size() + rawPages.size();
    }

    public boolean isEmpty()
    {
        return serializedPages.isEmpty() && rawPages.isEmpty();
    }
}
