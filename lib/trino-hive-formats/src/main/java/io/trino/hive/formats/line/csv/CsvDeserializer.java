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
package io.trino.hive.formats.line.csv;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.ByteSearch.indexOfByte;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Deserializer that is bug for bug compatible with OpenCSVSerde.
 */
// Note most of this code was forked from au.com.bytecode.opencsv.CSVParser and extensively rewritten
public class CsvDeserializer
        implements LineDeserializer
{
    private final List<Column> columns;
    private final char separatorChar;
    private final char quoteChar;
    private final char escapeChar;

    private static final int MISSING_FIELD = -1;

    private final StringBuilder buffer = new StringBuilder(1024);
    private final String[] rowValues;

    // a line with no quote and no escape character is a plain split on the separator, and can be
    // read straight from the line buffer without decoding a String for the line or for any field
    private final boolean byteLevelSplitPossible;
    private final byte separatorByte;
    private final byte quoteByte;
    private final byte escapeByte;
    private final int[] fieldOffsets;
    private final int[] fieldLengths;

    public CsvDeserializer(List<Column> columns, char separatorChar, char quoteChar, char escapeChar)
    {
        checkArgument(columns.size() == columns.stream().mapToInt(Column::ordinal).distinct().count(),
                "Columns cannot have duplicate ordinals: %s",
                columns);
        columns.forEach(column -> checkArgument(column.type() == VARCHAR, "CSV only supports VARCHAR columns: %s", column));
        this.columns = ImmutableList.copyOf(columns);

        int columnCount = this.columns.stream().mapToInt(Column::ordinal).max().orElse(-1) + 1;
        this.rowValues = new String[columnCount];

        checkArgument(separatorChar != '\0', "Separator cannot be the null character (ASCII 0)");
        checkArgument(separatorChar != quoteChar, "Separator and quote character cannot be the same");
        checkArgument(separatorChar != escapeChar, "Separator and escape character cannot be the same");

        // Quote and escape character can be the same when both are the null character (quoting and escaping are disabled)
        if (quoteChar != '\0' || escapeChar != '\0') {
            checkArgument(quoteChar != escapeChar, "Quote and escape character cannot be the same");
        }
        this.separatorChar = separatorChar;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;

        this.fieldOffsets = new int[columnCount];
        this.fieldLengths = new int[columnCount];
        // Only ASCII characters can be matched against UTF-8 bytes, because an ASCII byte never
        // occurs inside a multi byte UTF-8 sequence.
        this.byteLevelSplitPossible = separatorChar < 128 && quoteChar < 128 && escapeChar < 128;
        this.separatorByte = (byte) separatorChar;
        this.quoteByte = (byte) quoteChar;
        this.escapeByte = (byte) escapeChar;
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return Collections.nCopies(columns.size(), VARCHAR);
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
    {
        if (byteLevelSplitPossible && !lineBuffer.isEmpty() && !columns.isEmpty() && isUnquoted(lineBuffer)) {
            deserializeUnquoted(lineBuffer, builder);
            return;
        }

        parseLine(lineBuffer);

        builder.declarePosition();
        for (int i = 0; i < columns.size(); i++) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(i);
            String fieldValue = rowValues[columns.get(i).ordinal()];
            if (fieldValue == null) {
                blockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(fieldValue));
            }
        }
    }

    /**
     * A line with neither character present cannot take any of the quote or escape branches of
     * {@link #parseLine}, so it reduces to a plain split on the separator.
     */
    private boolean isUnquoted(LineBuffer lineBuffer)
    {
        byte[] buffer = lineBuffer.getBuffer();
        int length = lineBuffer.getLength();
        return indexOfByte(buffer, 0, length, quoteByte, escapeByte) < 0;
    }

    private void deserializeUnquoted(LineBuffer lineBuffer, PageBuilder builder)
    {
        byte[] buffer = lineBuffer.getBuffer();
        int end = lineBuffer.getLength();

        Arrays.fill(fieldLengths, MISSING_FIELD);
        int fieldIndex = 0;
        int offset = 0;
        while (fieldIndex < fieldLengths.length) {
            int separatorOffset = indexOfByte(buffer, offset, end, separatorByte);
            if (separatorOffset < 0) {
                break;
            }
            fieldOffsets[fieldIndex] = offset;
            fieldLengths[fieldIndex] = separatorOffset - offset;
            fieldIndex++;
            offset = separatorOffset + 1;
        }
        // a trailing unterminated field is still a value, but fields beyond the last column are dropped
        if (fieldIndex < fieldLengths.length) {
            fieldOffsets[fieldIndex] = offset;
            fieldLengths[fieldIndex] = end - offset;
        }

        Slice line = Slices.wrappedBuffer(buffer, 0, end);
        builder.declarePosition();
        for (int i = 0; i < columns.size(); i++) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(i);
            int ordinal = columns.get(i).ordinal();
            int length = fieldLengths[ordinal];
            if (length == MISSING_FIELD) {
                blockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(blockBuilder, line, fieldOffsets[ordinal], length);
            }
        }
    }

    private void parseLine(LineBuffer lineBuffer)
    {
        requireNonNull(lineBuffer, "lineBuffer is null");

        Arrays.fill(rowValues, null);
        buffer.setLength(0);

        // empty line results in all null values
        if (lineBuffer.isEmpty() || columns.isEmpty()) {
            return;
        }

        String line = new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8);

        int columnIndex = 0;
        boolean inQuotes = false;
        boolean inField = false;

        int position = 0;
        while (position < line.length() && columnIndex < rowValues.length) {
            char c = line.charAt(position);
            if (c == escapeChar) {
                // if the next character is special, process it here as to not trigger the special handling
                if (inQuotes || inField) {
                    int nextCharacter = peekNextCharacter(line, position);
                    if (nextCharacter == quoteChar || nextCharacter == escapeChar) {
                        buffer.append(line.charAt(position + 1));
                        position++;
                    }
                }
            }
            else if (c == quoteChar) {
                // a quote character can be escaped with another quote character
                if ((inQuotes || inField) && peekNextCharacter(line, position) == quoteChar) {
                    buffer.append(line.charAt(position + 1));
                    position++;
                }
                else {
                    // the tricky case of an embedded quote in the middle: a,bc"d"ef,g
                    // Embedded quote is not for first 3 characters of the line, and is not allowed immediately before a separator
                    if (position > 2 &&
                            line.charAt(position - 1) != separatorChar &&
                            line.length() > (position + 1) &&
                            line.charAt(position + 1) != separatorChar) {
                        // if field starts begins whitespace, skip the whitespace and quote
                        if (!buffer.isEmpty() && isAllWhitespace(buffer)) {
                            buffer.setLength(0);
                        }
                        else {
                            // otherwise write the quote as a literal value
                            buffer.append(c);
                        }
                    }
                    inQuotes = !inQuotes;
                }
                inField = !inField;
            }
            else if (c == separatorChar && !inQuotes) {
                // end of a value
                rowValues[columnIndex] = buffer.toString();
                columnIndex++;
                buffer.setLength(0);
                inField = false;
            }
            else {
                buffer.append(c);
                inField = true;
            }
            position++;
        }

        // if last field is an unterminated field, ignore the value
        if (columnIndex < rowValues.length && !inQuotes) {
            rowValues[columnIndex] = buffer.toString();
        }
        buffer.setLength(0);
    }

    private static int peekNextCharacter(String line, int position)
    {
        return line.length() > position + 1 ? line.charAt(position + 1) : -1;
    }

    private static boolean isAllWhitespace(CharSequence sequence)
    {
        for (int i = 0; i < sequence.length(); i++) {
            if (!Character.isWhitespace(sequence.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
