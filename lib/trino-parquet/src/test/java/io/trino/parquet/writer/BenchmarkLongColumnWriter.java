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
package io.trino.parquet.writer;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Param;

import java.util.Random;

import static io.trino.parquet.reader.TestData.randomLong;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class BenchmarkLongColumnWriter
        extends AbstractColumnWriterBenchmark
{
    private static final Random RANDOM = new Random(23423523L);

    @Param({
            "0", "20", "35", "50", "64"
    })
    public int bitWidth;

    @Override
    protected Type getTrinoType()
    {
        return BIGINT;
    }

    @Override
    protected PrimitiveType getParquetType()
    {
        return Types.optional(INT64).named("name");
    }

    @Override
    protected Block generateBlock(int size)
    {
        BlockBuilder blockBuilder = getTrinoType().createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), size);
        for (long value : this.generateDataBatch(size)) {
            getTrinoType().writeLong(blockBuilder, value);
        }
        return blockBuilder.buildValueBlock();
    }

    private long[] generateDataBatch(int size)
    {
        long[] batch = new long[size];
        if (bitWidth == 0) {
            for (int i = 0; i < size; i++) {
                batch[i] = i;
            }
        }
        else {
            for (int i = 0; i < size; i++) {
                batch[i] = randomLong(RANDOM, bitWidth);
            }
        }
        return batch;
    }

    public static void main(String[] args)
            throws Exception
    {
        run(BenchmarkLongColumnWriter.class);
    }
}
