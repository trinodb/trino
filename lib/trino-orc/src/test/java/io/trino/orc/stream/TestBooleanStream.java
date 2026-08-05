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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDecompressor;
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcDecompressor.createOrcDecompressor;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBooleanStream
        extends AbstractTestValueStream<Boolean, BooleanStreamCheckpoint, BooleanOutputStream, BooleanInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Boolean>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Boolean> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add(i % 3 == 0);
            }
            groups.add(group);
        }
        List<Boolean> group = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            group.add(i % 3 == 0);
        }
        groups.add(group);
        testWriteValue(groups);
    }

    @Test
    public void testWriteMultiple()
            throws IOException
    {
        BooleanOutputStream outputStream = createValueOutputStream();
        for (int i = 0; i < 3; i++) {
            outputStream.reset();

            BooleanList expectedValues = new BooleanArrayList(1024);
            outputStream.writeBooleans(32, true);
            expectedValues.addAll(Collections.nCopies(32, true));
            outputStream.writeBooleans(32, false);
            expectedValues.addAll(Collections.nCopies(32, false));

            outputStream.writeBooleans(1, true);
            expectedValues.add(true);
            outputStream.writeBooleans(1, false);
            expectedValues.add(false);

            outputStream.writeBooleans(34, true);
            expectedValues.addAll(Collections.nCopies(34, true));
            outputStream.writeBooleans(34, false);
            expectedValues.addAll(Collections.nCopies(34, false));

            outputStream.writeBoolean(true);
            expectedValues.add(true);
            outputStream.writeBoolean(false);
            expectedValues.add(false);

            outputStream.close();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
            StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(new OrcColumnId(33));
            streamDataOutput.writeData(sliceOutput);
            Stream stream = streamDataOutput.getStream();
            assertThat(stream.getStreamKind()).isEqualTo(StreamKind.DATA);
            assertThat(stream.getColumnId()).isEqualTo(new OrcColumnId(33));
            assertThat(stream.getLength()).isEqualTo(sliceOutput.size());

            BooleanInputStream valueStream = createValueStream(sliceOutput.slice());
            for (int index = 0; index < expectedValues.size(); index++) {
                boolean expectedValue = expectedValues.getBoolean(index);
                boolean actualValue = readValue(valueStream);
                assertThat(actualValue).isEqualTo(expectedValue);
            }
        }
    }

    @Test
    public void testGetSetBitsAsBitmap()
            throws IOException
    {
        BooleanList expectedValues = new BooleanArrayList(256);
        BooleanOutputStream outputStream = createValueOutputStream();
        for (int i = 0; i < 256; i++) {
            boolean value = i % 5 == 0 || i % 7 == 0 || i % 64 == 63;
            outputStream.writeBoolean(value);
            expectedValues.add(value);
        }
        outputStream.close();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        outputStream.getStreamDataOutput(new OrcColumnId(33)).writeData(sliceOutput);
        Slice slice = sliceOutput.slice();

        assertGetSetBitsAsBitmap(slice, expectedValues, 0, 130);
        assertGetSetBitsAsBitmap(slice, expectedValues, 3, 127);
        assertGetSetBitsAsBitmap(slice, expectedValues, 9, 64);
        assertGetSetBitsAsBitmap(slice, expectedValues, 71, 85);
    }

    @Override
    protected BooleanOutputStream createValueOutputStream()
    {
        return new BooleanOutputStream(SNAPPY, COMPRESSION_BLOCK_SIZE);
    }

    @Override
    protected void writeValue(BooleanOutputStream outputStream, Boolean value)
    {
        outputStream.writeBoolean(value);
    }

    @Override
    protected BooleanInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        return new BooleanInputStream(new OrcInputStream(OrcChunkLoader.create(ORC_DATA_SOURCE_ID, slice, orcDecompressor, newSimpleAggregatedMemoryContext())));
    }

    @Override
    protected Boolean readValue(BooleanInputStream valueStream)
            throws IOException
    {
        return valueStream.nextBit();
    }

    private void assertGetSetBitsAsBitmap(Slice slice, BooleanList expectedValues, int skip, int batchSize)
            throws IOException
    {
        BooleanInputStream valueStream = createValueStream(slice);
        valueStream.skip(skip);

        long[] bitmap = new long[wordsForBits(batchSize)];
        int setCount = valueStream.getSetBits(batchSize, bitmap);

        int expectedSetCount = 0;
        for (int position = 0; position < batchSize; position++) {
            boolean expected = expectedValues.getBoolean(skip + position);
            expectedSetCount += expected ? 1 : 0;
            assertThat(isSet(bitmap, 0, position)).isEqualTo(expected);
        }
        assertThat(setCount).isEqualTo(expectedSetCount);
    }
}
