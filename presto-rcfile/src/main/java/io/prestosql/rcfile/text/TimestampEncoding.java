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
import io.prestosql.spi.type.Type;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.base.type.PrestoTimestampEncoderFactory.createTimestampEncoder;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TimestampEncoding
        implements TextColumnEncoding
{
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;

    static {
        @SuppressWarnings("SpellCheckingInspection")
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSSSS").getParser(),
        };
        @SuppressWarnings("SpellCheckingInspection")
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter();
        HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();
    }

    private final TimestampType type;
    private final Slice nullSequence;
    private final PrestoTimestampEncoder<?> prestoTimestampEncoder;
    private final StringBuilder buffer = new StringBuilder();

    public TimestampEncoding(Type type, Slice nullSequence)
    {
        requireNonNull(type, "type is null");
        verify(type instanceof TimestampType, "type is not a TimestampType");
        this.type = (TimestampType) type;
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
                HIVE_TIMESTAMP_PARSER.printTo(buffer, millis);
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
        HIVE_TIMESTAMP_PARSER.printTo(buffer, millis);
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
        long millis = HIVE_TIMESTAMP_PARSER.parseMillis(new String(slice.getBytes(offset, length), US_ASCII));
        long epochSeconds = floorDiv(millis, MILLISECONDS_PER_SECOND);
        return new DecodedTimestamp(
                epochSeconds,
                (int) (millis - epochSeconds * MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND);
    }
}
