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

public class DoubleFlatBatchReader
        extends AbstractBatchPrimitiveColumnReader
{
    protected long[] values;

    public DoubleFlatBatchReader(ColumnDescriptor descriptor)
    {
        this.columnDescriptor = requireNonNull(descriptor, "columnDescriptor is null");
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

    double[] tmpArr;

    // Not efficient to convert doubles to ints here. Would be nice to have an actual DoubleArrayBlock or the ability
    // to read doubles as longs.
    @Override
    protected void readValues(int valuePosition, int chunkSize)
    {
        // Retain a temporary array to hold the doubles. It's probably faster to convert smaller
        // chunk-sized blocks in a reused array than creating (and spending the time clearing) a whole batch-sized
        // array at the end. This also avoids wasting time converting entries that correspond to nulls.
        if (tmpArr == null || tmpArr.length < chunkSize) {
            tmpArr = new double[chunkSize];
        }
        valuesReader.readDoubles(tmpArr, 0, chunkSize);
        for (int i = 0; i < chunkSize; i++) {
            values[valuePosition++] = Double.doubleToLongBits(tmpArr[i]);
        }
    }
}
