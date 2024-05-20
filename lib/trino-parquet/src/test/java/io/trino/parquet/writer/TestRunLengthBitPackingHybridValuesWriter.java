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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.RleBitPackingHybridDecoder;
import io.trino.parquet.writer.valuewriter.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.bytes.BytesInput;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRunLengthBitPackingHybridValuesWriter
{
    @Test
    public void verifyRoundTrip()
            throws Exception
    {
        for (int i = 0; i <= 32; i++) {
            verifyRoundTrip(i, false);
            verifyRoundTrip(i, true);
        }
    }

    private static void verifyRoundTrip(int bitWidth, boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 64000);
        List<Integer> expected = generateInputValues(bitWidth);
        writeInput(expected, encoder, useWriteRepeated);
        BytesInput encodedBytes = encoder.toBytes();
        SimpleSliceInputStream input = new SimpleSliceInputStream(Slices.wrappedBuffer(encodedBytes.toByteArray()));

        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(bitWidth, true);
        decoder.init(input);
        int[] output = new int[expected.size()];
        decoder.read(output, 0, expected.size());
        assertThat(output).isEqualTo(expected.stream().mapToInt(Integer::intValue).toArray());
    }

    private static List<Integer> generateInputValues(int bitWidth)
    {
        long modValue = 1L << bitWidth;
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (int i = 0; i < 100; i++) {
            builder.add((int) (i % modValue));
        }
        for (int i = 0; i < 100; i++) {
            builder.add((int) (77 % modValue));
        }
        for (int i = 0; i < 100; i++) {
            builder.add((int) (88 % modValue));
        }
        for (int i = 0; i < 1000; i++) {
            builder.add((int) (i % modValue));
            builder.add((int) (i % modValue));
            builder.add((int) (i % modValue));
        }
        for (int i = 0; i < 1000; i++) {
            builder.add((int) (17 % modValue));
        }
        return builder.build();
    }

    private static void writeInput(List<Integer> input, RunLengthBitPackingHybridEncoder encoder, boolean useWriteRepeated)
            throws IOException
    {
        if (useWriteRepeated) {
            int previous = input.getFirst();
            int runLength = 1;
            for (int i = 1; i < input.size(); i++) {
                int current = input.get(i);
                if (previous != current) {
                    encoder.writeRepeatedInteger(previous, runLength);
                    previous = current;
                    runLength = 1;
                }
                else {
                    runLength++;
                }
            }
            encoder.writeRepeatedInteger(previous, runLength);
        }
        else {
            for (int value : input) {
                encoder.writeInt(value);
            }
        }
    }
}
