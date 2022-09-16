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
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LongFlatBatchReader
        extends AbstractBatchPrimitiveColumnReader
{
    protected long[] values;

    public LongFlatBatchReader(ColumnDescriptor descriptor)
    {
        this.columnDescriptor = requireNonNull(descriptor, "columnDescriptor is null");
    }

    // Wraps values and nulls arrays in ColumnChunk. Also converts to short and tiny int.
    @Override
    protected ColumnChunk makeColumnChunk(Field field, int totalNonNullCount, int batchSize, boolean[] isNull)
    {
        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, batchSize);
            return new ColumnChunk(block, emptyIntArray, emptyIntArray);
        }

        boolean hasNoNull = totalNonNullCount == batchSize;
        Optional<boolean[]> nullArr = hasNoNull ? Optional.empty() : Optional.of(isNull);
        Block block = new LongArrayBlock(batchSize, nullArr, values);

        return new ColumnChunk(block, emptyIntArray, emptyIntArray);
    }

    @Override
    protected void relocateNonnulls(int valuePosition, int chunkSize, int nonnullCount, boolean[] isNull)
    {
        int valueDestinationIndex = valuePosition + chunkSize - 1;
        int valueSourceIndex = valuePosition + nonnullCount - 1;
        while (valueDestinationIndex >= valuePosition) {
            if (!isNull[valueDestinationIndex]) {
                values[valueDestinationIndex] = values[valueSourceIndex];
                valueSourceIndex--;
            }
            valueDestinationIndex--;
        }
    }

    @Override
    protected void allocateValuesArray(int batchSize)
    {
        values = new long[nextBatchSize];
    }

    @Override
    protected void readValues(int valuePosition, int chunkSize)
    {
        valuesReader.readLongs(values, valuePosition, chunkSize);
    }
}
