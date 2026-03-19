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

import java.util.List;

import static java.util.Objects.requireNonNull;

public record BufferResult(
        long taskInstanceId,
        long token,
        long nextToken,
        boolean bufferComplete,
        List<Slice> serializedPages)
{
    public static BufferResult emptyResults(long taskInstanceId, long token, boolean bufferComplete)
    {
        return new BufferResult(taskInstanceId, token, token, bufferComplete, ImmutableList.of());
    }

    public BufferResult
    {
        serializedPages = ImmutableList.copyOf(requireNonNull(serializedPages, "serializedPages is null"));
    }

    public int size()
    {
        return serializedPages.size();
    }

    public boolean isEmpty()
    {
        return serializedPages.isEmpty();
    }
}
