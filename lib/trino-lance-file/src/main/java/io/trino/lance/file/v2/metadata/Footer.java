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

public record Footer(
        long columnMetadataStart,
        long columnMetadataOffsetsStart,
        long globalBuffOffsetStart,
        int numGlobalBuffers,
        int numColumns,
        short majorVersion,
        short minorVersion)
{
    public static final int COLUMN_METADATA_START_POS = 0;
    public static final int COLUMN_METADATA_OFFSETS_START_POS = COLUMN_METADATA_START_POS + 8;
    public static final int GLOBAL_BUFF_OFFSETS_START_POS = COLUMN_METADATA_OFFSETS_START_POS + 8;
    public static final int NUM_GLOBAL_BUFFERS_POS = GLOBAL_BUFF_OFFSETS_START_POS + 8;
    public static final int NUM_COLUMNS_POS = NUM_GLOBAL_BUFFERS_POS + 4;
    public static final int MAJOR_VERSION_POS = NUM_COLUMNS_POS + 4;
    public static final int MINOR_VERSION_POS = MAJOR_VERSION_POS + 2;

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
}
