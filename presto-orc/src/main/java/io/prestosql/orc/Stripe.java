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

import com.google.common.collect.ImmutableList;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.InputStreamSources;

import java.time.ZoneId;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stripe
{
    private final long rowCount;
    private final ZoneId fileTimeZone;
    private final ZoneId storageTimeZone;
    private final ColumnMetadata<ColumnEncoding> columnEncodings;
    private final List<RowGroup> rowGroups;
    private final InputStreamSources dictionaryStreamSources;

    public Stripe(long rowCount, ZoneId fileTimeZone, ZoneId storageTimeZone, ColumnMetadata<ColumnEncoding> columnEncodings, List<RowGroup> rowGroups, InputStreamSources dictionaryStreamSources)
    {
        this.rowCount = rowCount;
        this.fileTimeZone = requireNonNull(fileTimeZone, "fileTimeZone is null");
        this.storageTimeZone = requireNonNull(storageTimeZone, "storageTimeZone is null");
        this.columnEncodings = requireNonNull(columnEncodings, "columnEncodings is null");
        this.rowGroups = ImmutableList.copyOf(requireNonNull(rowGroups, "rowGroups is null"));
        this.dictionaryStreamSources = requireNonNull(dictionaryStreamSources, "dictionaryStreamSources is null");
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public ZoneId getFileTimeZone()
    {
        return fileTimeZone;
    }

    public ZoneId getStorageTimeZone()
    {
        return storageTimeZone;
    }

    public ColumnMetadata<ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }

    public List<RowGroup> getRowGroups()
    {
        return rowGroups;
    }

    public InputStreamSources getDictionaryStreamSources()
    {
        return dictionaryStreamSources;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("fileTimeZone", fileTimeZone)
                .add("storageTimeZone", storageTimeZone)
                .add("columnEncodings", columnEncodings)
                .add("rowGroups", rowGroups)
                .add("dictionaryStreams", dictionaryStreamSources)
                .toString();
    }
}
