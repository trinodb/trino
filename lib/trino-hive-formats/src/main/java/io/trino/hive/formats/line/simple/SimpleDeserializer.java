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
package io.trino.hive.formats.line.simple;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.encodings.text.TextColumnEncoding;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Deserializer that is bug for bug compatible with LazySimpleSerDe.
 */
public class SimpleDeserializer
        implements LineDeserializer
{
    private final List<Column> columns;
    private final int[] readColumnIndexes;
    private final TextColumnEncoding[] columnEncodings;

    private final Slice nullSequence;
    private final byte separator;
    private final Byte escapeByte;
    private final boolean lastColumnTakesRest;

    public SimpleDeserializer(List<Column> columns, TextEncodingOptions textEncodingOptions, int tableColumnCount)
    {
        this.columns = ImmutableList.copyOf(columns);
        nullSequence = textEncodingOptions.getNullSequence();
        separator = textEncodingOptions.getSeparators().getByte(0);
        escapeByte = textEncodingOptions.getEscapeByte();
        lastColumnTakesRest = textEncodingOptions.isLastColumnTakesRest();

        columnEncodings = new TextColumnEncoding[tableColumnCount];
        readColumnIndexes = new int[tableColumnCount];
        Arrays.fill(readColumnIndexes, -1);

        TextColumnEncodingFactory columnEncodingFactory = new TextColumnEncodingFactory(textEncodingOptions);
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            columnEncodings[column.ordinal()] = columnEncodingFactory.getEncoding(column.type());
            readColumnIndexes[column.ordinal()] = i;
        }
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return columns.stream()
                .map(Column::type)
                .collect(toImmutableList());
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
            throws IOException
    {
        builder.declarePosition();
        Slice line = Slices.wrappedBuffer(lineBuffer.getBuffer(), 0, lineBuffer.getLength());

        int offset = 0;
        int length = line.length();
        int end = offset + length;

        int elementOffset = offset;
        int fieldIndex = 0;
        while (offset < end) {
            byte currentByte = line.getByte(offset);
            if (currentByte == separator) {
                decodeElementValueInto(fieldIndex, builder, line, elementOffset, offset - elementOffset);
                elementOffset = offset + 1;
                fieldIndex++;
                if (lastColumnTakesRest && fieldIndex == columnEncodings.length - 1) {
                    // no need to process the remaining bytes as they are all assigned to the last column
                    break;
                }
            }
            else if (isEscapeByte(currentByte)) {
                // ignore the char after escape_char
                offset++;
            }
            offset++;
        }
        decodeElementValueInto(fieldIndex, builder, line, elementOffset, end - elementOffset);
        fieldIndex++;

        // missing fields are null
        while (fieldIndex < columnEncodings.length) {
            int ordinal = readColumnIndexes[fieldIndex];
            if (ordinal >= 0) {
                builder.getBlockBuilder(ordinal).appendNull();
            }
            fieldIndex++;
        }
    }

    private void decodeElementValueInto(int fieldIndex, PageBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        if (fieldIndex >= columnEncodings.length) {
            return;
        }
        TextColumnEncoding columnEncoding = columnEncodings[fieldIndex];
        if (columnEncoding == null) {
            return;
        }

        BlockBuilder blockBuilder = builder.getBlockBuilder(readColumnIndexes[fieldIndex]);
        if (isNullSequence(slice, offset, length)) {
            blockBuilder.appendNull();
        }
        else {
            columnEncoding.decodeValueInto(blockBuilder, slice, offset, length);
        }
    }

    private boolean isNullSequence(Slice slice, int offset, int length)
    {
        return nullSequence.equals(0, nullSequence.length(), slice, offset, length);
    }

    private boolean isEscapeByte(byte currentByte)
    {
        return escapeByte != null && currentByte == escapeByte;
    }
}
