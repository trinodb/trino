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
package io.trino.plugin.varada;

import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;

import java.util.Optional;

public class TestBlocks
{
    private TestBlocks()
    {
    }

    static IntArrayBlock intArrayBlock(Integer... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        int[] intValues = new int[posCount];
        for (int i = 0; i < values.length; ++i) {
            intValues[i] = values[i] != null ? values[i] : 0;
            valueIsNull[i] = (values[i] == null);
        }
        return new IntArrayBlock(posCount, Optional.of(valueIsNull), intValues);
    }

    static ShortArrayBlock shortArrayBlock(Number... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        short[] shortValues = new short[posCount];
        for (int i = 0; i < values.length; ++i) {
            shortValues[i] = values[i] != null ? values[i].shortValue() : 0;
            valueIsNull[i] = (values[i] == null);
        }
        return new ShortArrayBlock(posCount, Optional.of(valueIsNull), shortValues);
    }

    static ByteArrayBlock tinyintArrayBlock(Number... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        byte[] byteValues = new byte[posCount];
        for (int i = 0; i < values.length; ++i) {
            byteValues[i] = values[i] != null ? values[i].byteValue() : 0;
            valueIsNull[i] = (values[i] == null);
        }
        return new ByteArrayBlock(posCount, Optional.of(valueIsNull), byteValues);
    }

    static IntArrayBlock buildIntArrayBlock(int... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        return new IntArrayBlock(posCount, Optional.of(valueIsNull), values);
    }

    static LongArrayBlock buildLongArrayBlock(long... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        return new LongArrayBlock(posCount, Optional.of(valueIsNull), values);
    }

    static ByteArrayBlock buildByteArrayBlock(byte... values)
    {
        int posCount = values.length;
        boolean[] valueIsNull = new boolean[posCount];
        return new ByteArrayBlock(posCount, Optional.of(valueIsNull), values);
    }
}
