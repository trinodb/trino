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

import org.junit.jupiter.api.Test;

import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static org.assertj.core.api.Assertions.assertThat;

final class TestColumnAdapter
{
    @Test
    void testUnpackNullValuesWithTrailingNullInFragmentedWord()
    {
        int positionCount = Long.SIZE;
        int nonNullCount = positionCount / 2;
        long[] valueIsValid = alternatingValidity(positionCount);
        long[] source = LONG_ADAPTER.createTemporaryBuffer(0, nonNullCount, new long[0]);
        long[] destination = new long[positionCount];
        for (int index = 0; index < nonNullCount; index++) {
            source[index] = 100 + index;
        }

        LONG_ADAPTER.unpackNullValues(source, destination, valueIsValid, 0, nonNullCount, positionCount);

        for (int position = 0; position < positionCount; position += 2) {
            assertThat(destination[position]).isEqualTo(100 + (position / 2));
        }
    }

    @Test
    void testUnpackBinaryNullValuesWithTrailingNullInFragmentedWord()
    {
        int positionCount = Long.SIZE;
        int nonNullCount = positionCount / 2;
        long[] valueIsValid = alternatingValidity(positionCount);
        BinaryBuffer source = new BinaryBuffer(nonNullCount);
        BinaryBuffer destination = new BinaryBuffer(positionCount);
        for (int index = 0; index <= nonNullCount; index++) {
            source.getOffsets()[index] = index * 10;
        }

        BINARY_ADAPTER.unpackNullValues(source, destination, valueIsValid, 0, nonNullCount, positionCount);

        for (int position = 0; position <= positionCount; position++) {
            assertThat(destination.getOffsets()[position]).isEqualTo((position + 1) / 2 * 10);
        }
    }

    private static long[] alternatingValidity(int positionCount)
    {
        long[] valueIsValid = allocateWords(positionCount, false);
        for (int position = 0; position < positionCount; position += 2) {
            set(valueIsValid, 0, position);
        }
        return valueIsValid;
    }
}
