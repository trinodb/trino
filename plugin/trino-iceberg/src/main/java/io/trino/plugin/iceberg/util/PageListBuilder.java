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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class PageListBuilder
{
    private final int channels;
    private final PageBuilder pageBuilder;

    private ImmutableList.Builder<Page> pages;
    private int channel;

    public PageListBuilder(List<Type> types)
    {
        this.channels = types.size();
        this.pageBuilder = new PageBuilder(types);
        reset();
    }

    public void reset()
    {
        pages = ImmutableList.builder();
        pageBuilder.reset();
        channel = -1;
    }

    public List<Page> build()
    {
        checkArgument(channel == -1, "cannot be in row");
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
            pageBuilder.reset();
        }
        return pages.build();
    }

    public void beginRow()
    {
        checkArgument(channel == -1, "already in row");
        if (pageBuilder.isFull()) {
            pages.add(pageBuilder.build());
            pageBuilder.reset();
        }
        pageBuilder.declarePosition();
        channel = 0;
    }

    public void endRow()
    {
        checkArgument(channel == channels, "not at end of row");
        channel = -1;
    }

    public void appendNull()
    {
        nextColumn().appendNull();
    }

    public void appendInteger(int value)
    {
        INTEGER.writeLong(nextColumn(), value);
    }

    public void appendBigint(long value)
    {
        BIGINT.writeLong(nextColumn(), value);
    }

    public void appendTimestampTzMillis(long millisUtc, TimeZoneKey timeZoneKey)
    {
        TIMESTAMP_TZ_MILLIS.writeLong(nextColumn(), packDateTimeWithZone(millisUtc, timeZoneKey));
    }

    public void appendVarchar(String value)
    {
        VARCHAR.writeString(nextColumn(), value);
    }

    public void appendVarbinary(Slice value)
    {
        VARBINARY.writeSlice(nextColumn(), value);
    }

    public void appendIntegerArray(Iterable<Integer> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder array = column.beginBlockEntry();
        for (Integer value : values) {
            INTEGER.writeLong(array, value);
        }
        column.closeEntry();
    }

    public void appendBigintArray(Iterable<Long> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder array = column.beginBlockEntry();
        for (Long value : values) {
            BIGINT.writeLong(array, value);
        }
        column.closeEntry();
    }

    public void appendVarcharArray(Iterable<String> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder array = column.beginBlockEntry();
        for (String value : values) {
            VARCHAR.writeString(array, value);
        }
        column.closeEntry();
    }

    public void appendVarcharVarcharMap(Map<String, String> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder map = column.beginBlockEntry();
        values.forEach((key, value) -> {
            VARCHAR.writeString(map, key);
            VARCHAR.writeString(map, value);
        });
        column.closeEntry();
    }

    public void appendIntegerBigintMap(Map<Integer, Long> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder map = column.beginBlockEntry();
        values.forEach((key, value) -> {
            INTEGER.writeLong(map, key);
            BIGINT.writeLong(map, value);
        });
        column.closeEntry();
    }

    public void appendIntegerVarcharMap(Map<Integer, String> values)
    {
        BlockBuilder column = nextColumn();
        BlockBuilder map = column.beginBlockEntry();
        values.forEach((key, value) -> {
            INTEGER.writeLong(map, key);
            VARCHAR.writeString(map, value);
        });
        column.closeEntry();
    }

    public BlockBuilder nextColumn()
    {
        int currentChannel = channel;
        channel++;
        return pageBuilder.getBlockBuilder(currentChannel);
    }

    public static PageListBuilder forTable(ConnectorTableMetadata table)
    {
        return new PageListBuilder(table.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList()));
    }
}
