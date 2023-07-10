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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PipelinedBufferInfo
{
    private final OutputBufferId bufferId;
    private final long rowsAdded;
    private final long pagesAdded;
    private final int bufferedPages;
    private final long bufferedBytes;
    private final long pagesSent;
    private final boolean finished;

    @JsonCreator
    public PipelinedBufferInfo(
            @JsonProperty("bufferId") OutputBufferId bufferId,
            @JsonProperty("rowsAdded") long rowsAdded,
            @JsonProperty("pagesAdded") long pagesAdded,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("pagesSent") long pagesSent,
            @JsonProperty("finished") boolean finished)
    {
        this.bufferId = requireNonNull(bufferId, "bufferId is null");
        checkArgument(rowsAdded >= 0, "rowsAdded must be >= 0");
        this.rowsAdded = rowsAdded;
        checkArgument(pagesAdded >= 0, "pagesAdded must be >= 0");
        this.pagesAdded = pagesAdded;
        checkArgument(bufferedPages >= 0, "bufferedPages must be >= 0");
        this.bufferedPages = bufferedPages;
        checkArgument(bufferedBytes >= 0, "bufferedBytes must be >= 0");
        this.bufferedBytes = bufferedBytes;
        checkArgument(pagesSent >= 0, "pagesSent must be >= 0");
        this.pagesSent = pagesSent;
        this.finished = finished;
    }

    @JsonProperty
    public OutputBufferId getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public long getRowsAdded()
    {
        return rowsAdded;
    }

    @JsonProperty
    public long getPagesAdded()
    {
        return pagesAdded;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public long getPagesSent()
    {
        return pagesSent;
    }

    @JsonProperty
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelinedBufferInfo that = (PipelinedBufferInfo) o;
        return rowsAdded == that.rowsAdded &&
                pagesAdded == that.pagesAdded &&
                bufferedPages == that.bufferedPages &&
                bufferedBytes == that.bufferedBytes &&
                pagesSent == that.pagesSent &&
                finished == that.finished &&
                Objects.equals(bufferId, that.bufferId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bufferId, rowsAdded, pagesAdded, bufferedPages, bufferedBytes, pagesSent, finished);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("rowsAdded", rowsAdded)
                .add("pagesAdded", pagesAdded)
                .add("bufferedPages", bufferedPages)
                .add("bufferedBytes", bufferedBytes)
                .add("pagesSent", pagesSent)
                .add("finished", finished)
                .toString();
    }
}
