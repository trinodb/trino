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
package io.trino.orc.stream;

import io.airlift.slice.Slice;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDecompressor;
import io.trino.orc.checkpoint.DecimalStreamCheckpoint;
import io.trino.spi.type.Int128;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcDecompressor.createOrcDecompressor;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;

public class TestLongDecimalStream
        extends AbstractTestValueStream<Int128, DecimalStreamCheckpoint, DecimalOutputStream, DecimalInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        Random random = new Random(0);
        List<List<Int128>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Int128> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                BigInteger value = new BigInteger(120, random);
                group.add(Int128.valueOf(value));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected DecimalOutputStream createValueOutputStream()
    {
        return new DecimalOutputStream(SNAPPY, COMPRESSION_BLOCK_SIZE);
    }

    @Override
    protected void writeValue(DecimalOutputStream outputStream, Int128 value)
    {
        outputStream.writeUnscaledValue(value);
    }

    @Override
    protected DecimalInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        return new DecimalInputStream(OrcChunkLoader.create(ORC_DATA_SOURCE_ID, slice, orcDecompressor, newSimpleAggregatedMemoryContext()));
    }

    @Override
    protected Int128 readValue(DecimalInputStream valueStream)
            throws IOException
    {
        long[] decimal = new long[2];
        valueStream.nextLongDecimal(decimal, 1);
        return Int128.valueOf(decimal);
    }
}
