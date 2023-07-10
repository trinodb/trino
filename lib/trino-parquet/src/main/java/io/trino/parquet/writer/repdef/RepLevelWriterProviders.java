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
package io.trino.parquet.writer.repdef;

import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RepLevelWriterProviders
{
    private RepLevelWriterProviders() {}

    public static RepLevelWriterProvider of(Block block)
    {
        return new PrimitiveRepLevelWriterProvider(block);
    }

    public static RepLevelWriterProvider of(ColumnarRow columnarRow)
    {
        return new ColumnRowRepLevelWriterProvider(columnarRow);
    }

    public static RepLevelWriterProvider of(ColumnarArray columnarArray, int maxRepetitionLevel)
    {
        return new ColumnArrayRepLevelWriterProvider(columnarArray, maxRepetitionLevel);
    }

    public static RepLevelWriterProvider of(ColumnarMap columnarMap, int maxRepetitionLevel)
    {
        return new ColumnMapRepLevelWriterProvider(columnarMap, maxRepetitionLevel);
    }

    static class PrimitiveRepLevelWriterProvider
            implements RepLevelWriterProvider
    {
        private final Block block;

        PrimitiveRepLevelWriterProvider(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public RepetitionLevelWriter getRepetitionLevelWriter(Optional<RepetitionLevelWriter> nestedWriter, ValuesWriter encoder)
        {
            checkArgument(nestedWriter.isEmpty(), "nestedWriter should be empty for primitive repetition level writer");
            return new RepetitionLevelWriter()
            {
                private int offset;

                @Override
                public void writeRepetitionLevels(int parentLevel)
                {
                    writeRepetitionLevels(parentLevel, block.getPositionCount());
                }

                @Override
                public void writeRepetitionLevels(int parentLevel, int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, block.getPositionCount());
                    for (int i = 0; i < positionsCount; i++) {
                        encoder.writeInteger(parentLevel);
                    }
                    offset += positionsCount;
                }
            };
        }
    }

    static class ColumnRowRepLevelWriterProvider
            implements RepLevelWriterProvider
    {
        private final ColumnarRow columnarRow;

        ColumnRowRepLevelWriterProvider(ColumnarRow columnarRow)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
        }

        @Override
        public RepetitionLevelWriter getRepetitionLevelWriter(Optional<RepetitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column row repetition level writer");
            return new RepetitionLevelWriter()
            {
                private final RepetitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public void writeRepetitionLevels(int parentLevel)
                {
                    writeRepetitionLevels(parentLevel, columnarRow.getPositionCount());
                }

                @Override
                public void writeRepetitionLevels(int parentLevel, int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarRow.getPositionCount());
                    if (!columnarRow.mayHaveNull()) {
                        nestedWriter.writeRepetitionLevels(parentLevel, positionsCount);
                        offset += positionsCount;
                        return;
                    }

                    for (int position = offset; position < offset + positionsCount; ) {
                        if (columnarRow.isNull(position)) {
                            encoder.writeInteger(parentLevel);
                            position++;
                        }
                        else {
                            int consecutiveNonNullsCount = 1;
                            position++;
                            while (position < offset + positionsCount && !columnarRow.isNull(position)) {
                                position++;
                                consecutiveNonNullsCount++;
                            }
                            nestedWriter.writeRepetitionLevels(parentLevel, consecutiveNonNullsCount);
                        }
                    }
                    offset += positionsCount;
                }
            };
        }
    }

    static class ColumnMapRepLevelWriterProvider
            implements RepLevelWriterProvider
    {
        private final ColumnarMap columnarMap;
        private final int maxRepetitionLevel;

        ColumnMapRepLevelWriterProvider(ColumnarMap columnarMap, int maxRepetitionLevel)
        {
            this.columnarMap = requireNonNull(columnarMap, "columnarMap is null");
            this.maxRepetitionLevel = maxRepetitionLevel;
        }

        @Override
        public RepetitionLevelWriter getRepetitionLevelWriter(Optional<RepetitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column map repetition level writer");
            return new RepetitionLevelWriter()
            {
                private final RepetitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public void writeRepetitionLevels(int parentLevel)
                {
                    writeRepetitionLevels(parentLevel, columnarMap.getPositionCount());
                }

                @Override
                public void writeRepetitionLevels(int parentLevel, int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarMap.getPositionCount());
                    if (!columnarMap.mayHaveNull()) {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            writeNonNullableLevels(parentLevel, position);
                        }
                    }
                    else {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            if (columnarMap.isNull(position)) {
                                encoder.writeInteger(parentLevel);
                                continue;
                            }
                            writeNonNullableLevels(parentLevel, position);
                        }
                    }
                    offset += positionsCount;
                }

                private void writeNonNullableLevels(int parentLevel, int position)
                {
                    int entryLength = columnarMap.getEntryCount(position);
                    if (entryLength == 0) {
                        encoder.writeInteger(parentLevel);
                    }
                    else {
                        nestedWriter.writeRepetitionLevels(parentLevel, 1);
                        nestedWriter.writeRepetitionLevels(maxRepetitionLevel, entryLength - 1);
                    }
                }
            };
        }
    }

    static class ColumnArrayRepLevelWriterProvider
            implements RepLevelWriterProvider
    {
        private final ColumnarArray columnarArray;
        private final int maxRepetitionLevel;

        ColumnArrayRepLevelWriterProvider(ColumnarArray columnarArray, int maxRepetitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxRepetitionLevel = maxRepetitionLevel;
        }

        @Override
        public RepetitionLevelWriter getRepetitionLevelWriter(Optional<RepetitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column map repetition level writer");
            return new RepetitionLevelWriter()
            {
                private final RepetitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public void writeRepetitionLevels(int parentLevel)
                {
                    writeRepetitionLevels(parentLevel, columnarArray.getPositionCount());
                }

                @Override
                public void writeRepetitionLevels(int parentLevel, int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarArray.getPositionCount());
                    if (!columnarArray.mayHaveNull()) {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            writeNonNullableLevels(parentLevel, position);
                        }
                    }
                    else {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            if (columnarArray.isNull(position)) {
                                encoder.writeInteger(parentLevel);
                                continue;
                            }
                            writeNonNullableLevels(parentLevel, position);
                        }
                    }
                    offset += positionsCount;
                }

                private void writeNonNullableLevels(int parentLevel, int position)
                {
                    int arrayLength = columnarArray.getLength(position);
                    if (arrayLength == 0) {
                        encoder.writeInteger(parentLevel);
                    }
                    else {
                        nestedWriter.writeRepetitionLevels(parentLevel, 1);
                        nestedWriter.writeRepetitionLevels(maxRepetitionLevel, arrayLength - 1);
                    }
                }
            };
        }
    }

    private static void checkValidPosition(int offset, int positionsCount, int totalPositionsCount)
    {
        if (offset < 0 || positionsCount < 0 || offset + positionsCount > totalPositionsCount) {
            throw new IndexOutOfBoundsException(format("Invalid offset %s and positionsCount %s in block with %s positions", offset, positionsCount, totalPositionsCount));
        }
    }
}
