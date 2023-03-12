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
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

/**
 * Serializer that is bug for bug compatible with OpenCSVSerde.
 */
public class CsvSerializer
        implements LineSerializer
{
    private final List<Column> columns;
    private final byte separatorChar;
    private final byte quoteChar;
    private final int escapeChar;

    public CsvSerializer(List<Column> columns, byte separatorChar, byte quoteChar, byte escapeChar)
    {
        checkArgument(columns.size() == columns.stream().mapToInt(Column::ordinal).distinct().count(),
                "Columns cannot have duplicate ordinals: %s",
                columns);
        columns.forEach(column -> checkArgument(column.type() == VARCHAR, "CSV only supports VARCHAR columns: %s", column));
        this.columns = ImmutableList.copyOf(columns);

        checkArgument(separatorChar >= 0, format("Separator character must be 7-bit ASCII: %02x", separatorChar));
        checkArgument(quoteChar >= 0, format("Quote character must be 7-bit ASCII: %02x", quoteChar));
        checkArgument(escapeChar >= 0, format("Escape character must be 7-bit ASCII: %02x", escapeChar));
        checkArgument(separatorChar != '\0', "Separator can not be the null character (ASCII 0)");
        checkArgument(separatorChar != quoteChar, "Separator and quote character can not be the same");
        checkArgument(separatorChar != escapeChar, "Separator and escape character can not be the same");

        this.separatorChar = separatorChar;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return Collections.nCopies(columns.size(), VARCHAR);
    }

    @Override
    public void write(Page page, int position, SliceOutput sliceOutput)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel != 0) {
                sliceOutput.write(separatorChar);
            }
            Block block = page.getBlock(channel);
            if (!block.isNull(position)) {
                sliceOutput.write(quoteChar);

                Slice value = VARCHAR.getSlice(block, position);
                if (value.indexOfByte(quoteChar) < 0 && (escapeChar == quoteChar || value.indexOfByte(escapeChar) < 0)) {
                    sliceOutput.appendBytes(value);
                }
                else {
                    for (int i = 0; i < value.length(); i++) {
                        byte c = value.getByte(i);
                        if (c == escapeChar || c == quoteChar) {
                            sliceOutput.appendByte(escapeChar);
                        }
                        sliceOutput.appendByte(c);
                    }
                }

                sliceOutput.write(quoteChar);
            }
        }
    }
}
