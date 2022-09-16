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
package io.trino.parquet.batchreader;

import io.trino.parquet.Field;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.String.format;

public class Int64TimestampMillisFlatBatchReader
        extends LongFlatBatchReader
{
    public Int64TimestampMillisFlatBatchReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected ColumnChunk makeColumnChunk(Field field, int totalNonNullCount, int batchSize, boolean[] isNull)
    {
        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, batchSize);
            return new ColumnChunk(block, emptyIntArray, emptyIntArray);
        }

        boolean hasNoNull = totalNonNullCount == batchSize;
        Optional<boolean[]> nullArr = hasNoNull ? Optional.empty() : Optional.of(isNull);
        Block block;

        // TODO: Move conversion into readValues to avoid wasting time converting nulls
        Type type = field.getType();
        if (type instanceof TimestampWithTimeZoneType) {
            for (int i = 0; i < batchSize; i++) {
                values[i] = packDateTimeWithZone(values[i], UTC_KEY);
            }
            block = new LongArrayBlock(batchSize, nullArr, values);
        }
        else if (type instanceof TimestampType) {
            for (int i = 0; i < batchSize; i++) {
                values[i] *= MICROSECONDS_PER_MILLISECOND;
            }
            if (((TimestampType) type).isShort()) {
                block = new LongArrayBlock(batchSize, nullArr, values);
            }
            else {
                block = new Int96ArrayBlock(batchSize, nullArr, values, new int[batchSize]);
            }
        }
        else if (type == BIGINT) {
            block = new LongArrayBlock(batchSize, nullArr, values);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, columnDescriptor));
        }

        return new ColumnChunk(block, emptyIntArray, emptyIntArray);
    }
}
