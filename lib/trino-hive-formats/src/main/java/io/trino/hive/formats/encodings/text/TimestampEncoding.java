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
package io.trino.hive.formats.encodings.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.plugin.base.type.TrinoTimestampEncoder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TimestampType;

import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Function;

import static io.trino.hive.formats.HiveFormatUtils.createTimestampParser;
import static io.trino.hive.formats.HiveFormatUtils.formatHiveTimestamp;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TimestampEncoding
        implements TextColumnEncoding
{
    private final TimestampType type;
    private final Slice nullSequence;
    private final Function<String, DecodedTimestamp> timestampParser;
    private final TrinoTimestampEncoder<?> trinoTimestampEncoder;
    private final StringBuilder buffer = new StringBuilder();

    public TimestampEncoding(TimestampType type, Slice nullSequence, List<String> timestampFormats)
    {
        this.type = requireNonNull(type, "type is null");
        this.nullSequence = requireNonNull(nullSequence, "nullSequence is null");
        this.timestampParser = createTimestampParser(timestampFormats);
        this.trinoTimestampEncoder = createTimestampEncoder(type, UTC);
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValue(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        encodeValue(block, position, output);
    }

    private void encodeValue(Block block, int position, SliceOutput output)
    {
        buffer.setLength(0);
        formatHiveTimestamp(type, block, position, buffer);
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
                decodeValue(builder, slice, offset, length);
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        decodeValue(builder, slice, offset, length);
    }

    private void decodeValue(BlockBuilder builder, Slice slice, int offset, int length)
    {
        // Hive has a hard coded minimum length of 8 bytes
        if (length < 8) {
            builder.appendNull();
            return;
        }

        try {
            DecodedTimestamp decodedTimestamp = timestampParser.apply(slice.toStringAscii(offset, length));
            trinoTimestampEncoder.write(decodedTimestamp, builder);
        }
        catch (DateTimeParseException ignored) {
            builder.appendNull();
        }
    }
}
