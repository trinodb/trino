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

import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Param;

import java.util.Optional;
import java.util.Random;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public class BenchmarkBooleanColumnWriter
        extends AbstractColumnWriterBenchmark
{
    @Param({"0", "10", "50"})
    public int nullsPercentage;

    @Override
    protected Type getTrinoType()
    {
        return BOOLEAN;
    }

    @Override
    protected PrimitiveType getParquetType()
    {
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("name");
    }

    @Override
    protected Block generateBlock(int size)
    {
        Random random = new Random(23423523L);
        long[] values = Bitmap.allocateWords(size, false);
        long[] validity = Bitmap.allocateWords(size, false);
        for (int position = 0; position < size; position++) {
            if (random.nextBoolean()) {
                Bitmap.set(values, 0, position);
            }
            if (random.nextInt(100) >= nullsPercentage) {
                Bitmap.set(validity, 0, position);
            }
        }
        return new BitArrayBlock(size, nullsPercentage == 0 ? Optional.empty() : Optional.of(validity), values);
    }

    static void main()
            throws Exception
    {
        run(BenchmarkBooleanColumnWriter.class);
    }
}
