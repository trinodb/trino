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
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.encodings.text.TextColumnEncoding;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Serializer that is bug for bug compatible with LazySimpleSerDe.
 */
public class SimpleSerializer
        implements LineSerializer
{
    private final List<Column> columns;
    private final TextColumnEncoding[] columnEncodings;
    private final byte separator;
    private final Slice nullSequence;

    public SimpleSerializer(List<Column> columns, TextEncodingOptions options)
    {
        checkArgument(
                columns.size() == columns.stream().mapToInt(Column::ordinal).distinct().count(),
                "Columns cannot have duplicate ordinals: %s",
                columns);
        this.columns = ImmutableList.copyOf(columns);

        TextColumnEncodingFactory columnEncodingFactory = new TextColumnEncodingFactory(options);
        columnEncodings = columns.stream()
                .map(column -> columnEncodingFactory.getEncoding(column.type()))
                .toArray(TextColumnEncoding[]::new);
        separator = options.getSeparators().getByte(0);
        nullSequence = options.getNullSequence();
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return columns.stream()
                .map(Column::type)
                .collect(toImmutableList());
    }

    @Override
    public void write(Page page, int position, SliceOutput sliceOutput)
            throws IOException
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel > 0) {
                sliceOutput.appendByte(separator);
            }
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                sliceOutput.appendBytes(nullSequence);
            }
            else {
                columnEncodings[channel].encodeValueInto(block, position, sliceOutput);
            }
        }
    }
}
