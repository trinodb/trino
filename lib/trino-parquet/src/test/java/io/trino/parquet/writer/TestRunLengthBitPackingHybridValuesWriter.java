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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.trino.parquet.reader.TestData.UnsignedIntsGenerator;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRunLengthBitPackingHybridValuesWriter
{
    private static final Random RANDOM = new Random(10953676);

    @Test
    public void verifyRoundTrip()
            throws Exception
    {
        for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
            List<Integer> expected = generateInputValues(bitWidth);
            verifyRoundTrip(expected, bitWidth, false);
            verifyRoundTrip(expected, bitWidth, true);
        }
    }

    @Test
    public void verifyRoundTripRandomData()
            throws Exception
    {
        for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
            for (UnsignedIntsGenerator dataGenerator : UnsignedIntsGenerator.values()) {
                List<Integer> expected = Arrays.stream(dataGenerator.getData(20_000, bitWidth)).boxed().toList();
                verifyRoundTrip(expected, bitWidth, false);
                verifyRoundTrip(expected, bitWidth, true);
            }
        }
    }

    private static void verifyRoundTrip(List<Integer> inputValues, int bitWidth, boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 64000);
        writeInput(inputValues, encoder, useWriteRepeated);
        BytesInput encodedBytes = encoder.toBytes();
        SimpleSliceInputStream input = new SimpleSliceInputStream(Slices.wrappedBuffer(encodedBytes.toByteArray()));

        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(bitWidth, true);
        decoder.init(input);
        int[] output = new int[inputValues.size()];
        decoder.read(output, 0, inputValues.size());
        assertThat(output).isEqualTo(inputValues.stream().mapToInt(Integer::intValue).toArray());
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
                    // Split the run length into multiple calls to simulate real usage more closely
                    int splitRunLength = RANDOM.nextInt((int) (0.8 * runLength), runLength + 1);
                    encoder.writeRepeatedInteger(previous, splitRunLength);
                    encoder.writeRepeatedInteger(previous, runLength - splitRunLength);
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
