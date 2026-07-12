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
package io.trino.parquet.writer.valuewriter;

import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoBooleanPlainValuesWriter
{
    @Test
    public void testWriteBits()
            throws IOException
    {
        Random random = new Random(938472);
        for (int positionCount : new int[] {0, 1, 7, 8, 9, 63, 64, 65, 1023}) {
            TrinoBooleanPlainValuesWriter packedWriter = new TrinoBooleanPlainValuesWriter(1024 * 1024);
            BooleanPlainValuesWriter scalarWriter = new BooleanPlainValuesWriter();
            int position = 0;
            while (position < positionCount) {
                int bitCount = Math.min(random.nextInt(64) + 1, positionCount - position);
                long bits = random.nextLong();
                packedWriter.writeBits(bits, bitCount);
                for (int bit = 0; bit < bitCount; bit++) {
                    scalarWriter.writeBoolean((bits & (1L << bit)) != 0);
                }
                position += bitCount;
            }

            assertThat(packedWriter.getBytes().toByteArray()).isEqualTo(scalarWriter.getBytes().toByteArray());
        }
    }
}
