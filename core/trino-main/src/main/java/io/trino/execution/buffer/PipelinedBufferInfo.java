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

import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record PipelinedBufferInfo(
        OutputBufferId bufferId,
        long rowsAdded,
        long pagesAdded,
        int bufferedPages,
        long bufferedBytes,
        long pagesSent,
        boolean finished)
{
    public PipelinedBufferInfo
    {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(rowsAdded >= 0, "rowsAdded must be >= 0");
        checkArgument(pagesAdded >= 0, "pagesAdded must be >= 0");
        checkArgument(bufferedPages >= 0, "bufferedPages must be >= 0");
        checkArgument(bufferedBytes >= 0, "bufferedBytes must be >= 0");
        checkArgument(pagesSent >= 0, "pagesSent must be >= 0");
    }
}
