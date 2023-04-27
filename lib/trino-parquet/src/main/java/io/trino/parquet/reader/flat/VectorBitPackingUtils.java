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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;

import static io.trino.parquet.reader.flat.BitPackingUtils.bitCount;

public class VectorBitPackingUtils
{
    private static final ByteVector MASK_1 = ByteVector.broadcast(ByteVector.SPECIES_64, 1);
    private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(ByteVector.SPECIES_64, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, 0);

    private VectorBitPackingUtils() {}

    public static int vectorUnpackAndInvert8(boolean[] values, int offset, byte packedByte)
    {
        ByteVector.broadcast(ByteVector.SPECIES_64, packedByte)
                .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                .and(MASK_1)
                .lanewise(VectorOperators.NOT)
                .intoBooleanArray(values, offset);
        return bitCount(packedByte);
    }

    public static void vectorUnpack8FromByte(byte[] values, int offset, byte packedByte)
    {
        ByteVector.broadcast(ByteVector.SPECIES_64, packedByte)
                .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                .and(MASK_1)
                .intoArray(values, offset);
    }
}
