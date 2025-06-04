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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slices;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static io.trino.parquet.reader.TestData.generateMixedData;
import static io.trino.parquet.reader.flat.NullsDecoders.createNullsDecoder;
import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNullsDecoder
{
    private static final int N = 1000;
    private static final int MAX_MIXED_GROUP_SIZE = 23;
    private static final boolean[] ALL_NON_NULLS_ARRAY = new boolean[N];
    private static final boolean[] RANDOM_ARRAY = new boolean[N];
    private static final boolean[] MIXED_RANDOM_AND_GROUPED_ARRAY;

    static {
        Arrays.fill(ALL_NON_NULLS_ARRAY, true);
        Random r = new Random(0);
        for (int i = 0; i < N; i++) {
            RANDOM_ARRAY[i] = r.nextBoolean();
        }

        MIXED_RANDOM_AND_GROUPED_ARRAY = generateMixedData(r, N, MAX_MIXED_GROUP_SIZE);
    }

    @Test
    public void testDecoding()
            throws IOException
    {
        testDecoding(true);
        testDecoding(false);
    }

    private void testDecoding(boolean vectorizedDecodingEnabled)
            throws IOException
    {
        for (NullValuesProvider nullValuesProvider : NullValuesProvider.values()) {
            for (int batchSize : Arrays.asList(1, 3, 16, 100, 1000)) {
                boolean[] values = nullValuesProvider.getPositions();
                byte[] encoded = encode(values);
                FlatDefinitionLevelDecoder decoder = createNullsDecoder(vectorizedDecodingEnabled);
                decoder.init(Slices.wrappedBuffer(encoded));
                boolean[] result = new boolean[N];
                int nonNullCount = 0;
                for (int i = 0; i < N; i += batchSize) {
                    nonNullCount += decoder.readNext(result, i, min(batchSize, N - i));
                }
                // Parquet encodes whether value exists, Trino whether value is null
                boolean[] byteResult = flip(result);
                assertThat(byteResult).containsExactly(values);
                int expectedNonNull = nonNullCount(values);
                assertThat(nonNullCount).isEqualTo(expectedNonNull);
            }
        }
    }

    @Test
    public void testSkippedDecoding()
            throws IOException
    {
        testSkippedDecoding(true);
        testSkippedDecoding(false);
    }

    private void testSkippedDecoding(boolean vectorizedDecodingEnabled)
            throws IOException
    {
        for (NullValuesProvider nullValuesProvider : NullValuesProvider.values()) {
            for (int batchSize : Arrays.asList(1, 3, 16, 100, 1000)) {
                boolean[] values = nullValuesProvider.getPositions();
                byte[] encoded = encode(values);
                FlatDefinitionLevelDecoder decoder = createNullsDecoder(vectorizedDecodingEnabled);
                decoder.init(Slices.wrappedBuffer(encoded));
                int nonNullCount = 0;
                int numberOfBatches = (N + batchSize - 1) / batchSize;
                Random random = new Random(batchSize * 0xFFFFFFFFL * N);
                int skippedBatches = random.nextInt(numberOfBatches);
                int alreadyRead = 0;
                for (int i = 0; i < skippedBatches; i++) {
                    int chunkSize = min(batchSize, N - alreadyRead);
                    nonNullCount += decoder.skip(chunkSize);
                    alreadyRead += chunkSize;
                }
                assertThat(nonNullCount).isEqualTo(nonNullCount(values, alreadyRead));

                boolean[] result = new boolean[N - alreadyRead];
                boolean[] expected = Arrays.copyOfRange(values, alreadyRead, values.length);
                int offset = 0;
                while (alreadyRead < N) {
                    int chunkSize = min(batchSize, N - alreadyRead);
                    nonNullCount += decoder.readNext(result, offset, chunkSize);
                    alreadyRead += chunkSize;
                    offset += chunkSize;
                }
                // Parquet encodes whether value exists, Trino whether value is null
                boolean[] byteResult = flip(result);
                assertThat(byteResult).containsExactly(expected);

                assertThat(nonNullCount).isEqualTo(nonNullCount(values));
            }
        }
    }

    private enum NullValuesProvider
    {
        ALL_NULLS {
            @Override
            boolean[] getPositions()
            {
                return new boolean[N];
            }
        },
        ALL_NON_NULLS {
            @Override
            boolean[] getPositions()
            {
                return ALL_NON_NULLS_ARRAY;
            }
        },
        RANDOM {
            @Override
            boolean[] getPositions()
            {
                return RANDOM_ARRAY;
            }
        },
        MIXED_RANDOM_AND_GROUPED {
            @Override
            boolean[] getPositions()
            {
                return MIXED_RANDOM_AND_GROUPED_ARRAY;
            }
        };

        abstract boolean[] getPositions();
    }

    private static byte[] encode(boolean[] values)
            throws IOException
    {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(1, N, N, HeapByteBufferAllocator.getInstance());
        for (int i = 0; i < N; i++) {
            encoder.writeInt(values[i] ? 1 : 0);
        }
        return encoder.toBytes().toByteArray();
    }

    private static boolean[] flip(boolean[] values)
    {
        boolean[] result = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = !values[i];
        }
        return result;
    }

    private static int nonNullCount(boolean[] values)
    {
        return nonNullCount(values, values.length);
    }

    private static int nonNullCount(boolean[] values, int length)
    {
        int nonNullCount = 0;
        for (int i = 0; i < length; i++) {
            nonNullCount += values[i] ? 1 : 0;
        }
        return nonNullCount;
    }
}
