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
package io.prestosql.orc;

import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.Stream.StreamKind;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamId
{
    private final OrcColumnId columnId;
    private final StreamKind streamKind;

    public StreamId(Stream stream)
    {
        requireNonNull(stream, "stream is null");
        this.columnId = stream.getColumnId();
        this.streamKind = stream.getStreamKind();
    }

    public StreamId(OrcColumnId columnId, StreamKind streamKind)
    {
        this.columnId = columnId;
        this.streamKind = streamKind;
    }

    public OrcColumnId getColumnId()
    {
        return columnId;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnId, streamKind);
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
        StreamId streamId = (StreamId) o;
        return Objects.equals(columnId, streamId.columnId) &&
                streamKind == streamId.streamKind;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnId", columnId)
                .add("streamKind", streamKind)
                .toString();
    }
}
