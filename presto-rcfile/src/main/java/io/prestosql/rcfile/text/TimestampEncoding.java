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
package io.prestosql.rcfile.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.plugin.base.type.DecodedTimestamp;
import io.prestosql.plugin.base.type.PrestoTimestampEncoder;
import io.prestosql.rcfile.ColumnData;
import io.prestosql.rcfile.EncodeOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TimestampType;
import org.joda.time.format.DateTimeFormat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import static io.prestosql.plugin.base.type.PrestoTimestampEncoderFactory.createTimestampEncoder;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TimestampEncoding
        implements TextColumnEncoding
{
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-M-d[ H:m[:s]]")
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .toFormatter();
    // TODO: switch to java.time when we implement writes with variable precision
    private static final org.joda.time.format.DateTimeFormatter HIVE_TIMESTAMP_PRINTER =
            new org.joda.time.format.DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter()).toFormatter().withZoneUTC();

    private final TimestampType type;
    private final Slice nullSequence;
    private final PrestoTimestampEncoder<?> prestoTimestampEncoder;
    private final StringBuilder buffer = new StringBuilder();

    public TimestampEncoding(TimestampType type, Slice nullSequence)
    {
        this.type = requireNonNull(type, "type is null");
        this.nullSequence = nullSequence;
        prestoTimestampEncoder = createTimestampEncoder(this.type, UTC);
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                long millis = floorDiv(type.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
                buffer.setLength(0);
                HIVE_TIMESTAMP_PRINTER.printTo(buffer, millis);
                for (int index = 0; index < buffer.length(); index++) {
                    output.writeByte(buffer.charAt(index));
                }
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
    {
        long millis = floorDiv(type.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
        buffer.setLength(0);
        HIVE_TIMESTAMP_PRINTER.printTo(buffer, millis);
        for (int index = 0; index < buffer.length(); index++) {
            output.writeByte(buffer.charAt(index));
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                DecodedTimestamp decodedTimestamp = parseTimestamp(slice, offset, length);
                prestoTimestampEncoder.write(decodedTimestamp, builder);
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        DecodedTimestamp decodedTimestamp = parseTimestamp(slice, offset, length);
        prestoTimestampEncoder.write(decodedTimestamp, builder);
    }

    private static DecodedTimestamp parseTimestamp(Slice slice, int offset, int length)
    {
        String timestamp = new String(slice.getBytes(offset, length), US_ASCII);
        LocalDateTime localDateTime = LocalDateTime.parse(timestamp, HIVE_TIMESTAMP_PARSER);
        return new DecodedTimestamp(localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
    }
}
