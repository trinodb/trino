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
package io.trino.lance.file.v2.metadata;

import io.airlift.slice.Slice;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class Footer
{
    public static final int COLUMN_METADATA_START_POS = 0;
    public static final int COLUMN_METADATA_OFFSETS_START_POS = COLUMN_METADATA_START_POS + 8;
    public static final int GLOBAL_BUFF_OFFSETS_START_POS = COLUMN_METADATA_OFFSETS_START_POS + 8;
    public static final int NUM_GLOBAL_BUFFERS_POS = GLOBAL_BUFF_OFFSETS_START_POS + 8;
    public static final int NUM_COLUMNS_POS = NUM_GLOBAL_BUFFERS_POS + 4;
    public static final int MAJOR_VERSION_POS = NUM_COLUMNS_POS + 4;
    public static final int MINOR_VERSION_POS = MAJOR_VERSION_POS + 2;

    private final long columnMetadataStart;
    private final long columnMetadataOffsetsStart;
    private final long globalBuffOffsetStart;
    private final int numGlobalBuffers;
    private final int numColumns;
    private final short majorVersion;
    private final short minorVersion;

    public Footer(
            long columnMetadataStart,
            long columnMetadataOffsetsStart,
            long globalBuffOffsetStart,
            int numGlobalBuffers,
            int numColumns,
            short majorVersion,
            short minorVersion)
    {
        this.columnMetadataStart = columnMetadataStart;
        this.columnMetadataOffsetsStart = columnMetadataOffsetsStart;
        this.globalBuffOffsetStart = globalBuffOffsetStart;
        this.numGlobalBuffers = numGlobalBuffers;
        this.numColumns = numColumns;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public static Footer from(Slice data)
    {
        long columnMetaStart = data.getLong(COLUMN_METADATA_START_POS);
        long columnMetaOffsetsStart = data.getLong(COLUMN_METADATA_OFFSETS_START_POS);
        long globalBuffOffsetStart = data.getLong(GLOBAL_BUFF_OFFSETS_START_POS);
        int numGlobalBuffers = data.getInt(NUM_GLOBAL_BUFFERS_POS);
        int numColumns = data.getInt(NUM_COLUMNS_POS);
        short majorVersion = data.getShort(MAJOR_VERSION_POS);
        short minorVersion = data.getShort(MINOR_VERSION_POS);
        return new Footer(columnMetaStart, columnMetaOffsetsStart, globalBuffOffsetStart, numGlobalBuffers, numColumns, majorVersion, minorVersion);
    }

    public long getColumnMetadataStart()
    {
        return columnMetadataStart;
    }

    public long getColumnMetadataOffsetsStart()
    {
        return columnMetadataOffsetsStart;
    }

    public long getGlobalBuffOffsetStart()
    {
        return globalBuffOffsetStart;
    }

    public int getNumGlobalBuffers()
    {
        return numGlobalBuffers;
    }

    public int getNumColumns()
    {
        return numColumns;
    }

    public int getMajorVersion()
    {
        return majorVersion;
    }

    public int getMinorVersion()
    {
        return minorVersion;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnMetadataStart", columnMetadataStart)
                .add("columnMetadataOffsetsStart", columnMetadataOffsetsStart)
                .add("globalBuffOffsetStart", globalBuffOffsetStart)
                .add("numGlobalBuffers", numGlobalBuffers)
                .add("numColumns", numColumns)
                .add("majorVersion", majorVersion)
                .add("minorVersion", minorVersion)
                .toString();
    }
}
