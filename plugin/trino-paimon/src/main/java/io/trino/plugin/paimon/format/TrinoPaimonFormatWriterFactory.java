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
package io.trino.plugin.paimon.format;

import io.trino.spi.type.Type;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

class TrinoPaimonFormatWriterFactory
        implements FormatWriterFactory
{
    private final String formatIdentifier;
    private final RowType rowType;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;
    private final int writeBatchSize;
    private final TrinoPaimonFormatWriterOptions writerOptions;

    TrinoPaimonFormatWriterFactory(
            String formatIdentifier,
            RowType rowType,
            int writeBatchSize,
            TrinoPaimonFormatWriterOptions writerOptions)
    {
        this.formatIdentifier = requireNonNull(formatIdentifier, "formatIdentifier is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.columnNames = List.copyOf(rowType.getFieldNames());
        this.columnTypes = List.copyOf(TrinoPaimonFileFormat.trinoTypes(rowType));
        this.logicalTypes = rowType.getFields().stream()
                .map(field -> field.type())
                .toList();
        this.writeBatchSize = writeBatchSize;
        this.writerOptions = requireNonNull(writerOptions, "writerOptions is null");
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression)
            throws IOException
    {
        return new TrinoPaimonFormatWriter(
                formatIdentifier,
                rowType,
                columnNames,
                columnTypes,
                logicalTypes,
                writeBatchSize,
                writerOptions,
                requireNonNull(out, "out is null"),
                requireNonNull(compression, "compression is null"));
    }
}
