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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ParquetDecodingException;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalConversions.shortToLongCast;
import static io.trino.spi.type.DecimalConversions.shortToShortCast;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Int64DecimalBatchReader
        extends LongFlatBatchReader
{
    private final DecimalType parquetDecimalType;

    public Int64DecimalBatchReader(ColumnDescriptor descriptor, DecimalType parquetDecimalType)
    {
        super(descriptor);
        this.parquetDecimalType = requireNonNull(parquetDecimalType, "parquetDecimalType is null");
    }

    @Override
    protected ColumnChunk makeColumnChunk(Field field, int totalNonNullCount, int batchSize, boolean[] isNull)
    {
        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, batchSize);
            return new ColumnChunk(block, emptyIntArray, emptyIntArray);
        }

        Type trinoType = field.getType();
        if (!((trinoType instanceof DecimalType) || isIntegerType(trinoType))) {
            throw new ParquetDecodingException(format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
        }

        boolean hasNoNull = totalNonNullCount == batchSize;
        Optional<boolean[]> nullArr = hasNoNull ? Optional.empty() : Optional.of(isNull);
        Block block;

        if (trinoType instanceof DecimalType) {
            DecimalType trinoDecimalType = (DecimalType) trinoType;

            if (isShortDecimal(trinoDecimalType)) {
                long rescale = longTenToNth(Math.abs(trinoDecimalType.getScale() - parquetDecimalType.getScale()));
                long rescaled2 = rescale / 2;
                int pp = parquetDecimalType.getPrecision();
                int ps = parquetDecimalType.getScale();
                int tp = trinoDecimalType.getPrecision();
                int ts = trinoDecimalType.getScale();
                long[] convertedValues = new long[values.length];
                for (int i = 0; i < batchSize; i++) {
                    convertedValues[i] = shortToShortCast(values[i], pp, ps, tp, ts, rescale, rescaled2);
                }
                block = new LongArrayBlock(batchSize, nullArr, convertedValues);
            }
            else if (isLongDecimal(trinoDecimalType)) {
                int pp = parquetDecimalType.getPrecision();
                int ps = parquetDecimalType.getScale();
                int tp = trinoDecimalType.getPrecision();
                int ts = trinoDecimalType.getScale();
                long[] convertedValues = new long[values.length * 2];
                for (int i = 0; i < batchSize; i++) {
                    Int128 convVal = shortToLongCast(values[i], pp, ps, tp, ts);
                    convertedValues[i * 2] = convVal.getHigh();
                    convertedValues[i * 2 + 1] = convVal.getLow();
                }
                block = new Int128ArrayBlock(batchSize, nullArr, convertedValues);
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
            }
        }
        else {
            if (parquetDecimalType.getScale() != 0) {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
            }

            if (trinoType.equals(BIGINT)) {
                block = new LongArrayBlock(batchSize, nullArr, values);
            }
            else if (trinoType.equals(INTEGER)) {
                int[] intValues = new int[batchSize];
                copyLongToInt(values, intValues);
                block = new IntArrayBlock(batchSize, nullArr, intValues);
            }
            else if (trinoType.equals(SMALLINT)) {
                short[] shortValues = new short[batchSize];
                copyLongToShort(values, shortValues);
                block = new ShortArrayBlock(batchSize, nullArr, shortValues);
            }
            else if (trinoType.equals(TINYINT)) {
                byte[] byteValues = new byte[batchSize];
                copyLongToByte(values, byteValues);
                block = new ByteArrayBlock(batchSize, nullArr, byteValues);
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
            }
        }

        return new ColumnChunk(block, emptyIntArray, emptyIntArray);
    }
}
