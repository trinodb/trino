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
package io.prestosql.orc.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stream
{
    public enum StreamKind
    {
        PRESENT,
        DATA,
        LENGTH,
        DICTIONARY_DATA,
        DICTIONARY_COUNT,
        SECONDARY,
        ROW_INDEX,
        BLOOM_FILTER,
        BLOOM_FILTER_UTF8,
    }

    private final OrcColumnId columnId;
    private final StreamKind streamKind;
    private final int length;
    private final boolean useVInts;

    public Stream(OrcColumnId columnId, StreamKind streamKind, int length, boolean useVInts)
    {
        this.columnId = columnId;
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.length = length;
        this.useVInts = useVInts;
    }

    public OrcColumnId getColumnId()
    {
        return columnId;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    public int getLength()
    {
        return length;
    }

    public boolean isUseVInts()
    {
        return useVInts;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", columnId)
                .add("streamKind", streamKind)
                .add("length", length)
                .add("useVInts", useVInts)
                .toString();
    }
}
