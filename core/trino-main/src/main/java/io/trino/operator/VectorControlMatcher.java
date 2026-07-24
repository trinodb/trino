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
package io.trino.operator;

import com.google.common.base.StandardSystemProperty;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import static java.util.Locale.ENGLISH;

/**
 * Matches thirty two control slots at a time with a single vector comparison, four times as many as
 * the scalar matcher. The match set holds one bit per matching slot.
 * <p>
 * The comparison lowers to {@code pcmpeqb} plus {@code pmovmskb} on x86, and to {@code cmeq} plus a
 * mask reduction on ARM. The species has to be a constant of the class using it, otherwise the JVM
 * cannot compile the comparison into a vector instruction at all, so the width cannot be chosen at
 * runtime: hardware that has no 256 bit vector register uses the scalar matcher instead of a
 * narrower vector.
 */
final class VectorControlMatcher
        implements ControlMatcher
{
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    public static boolean isSupported()
    {
        // A vector wider than the hardware register is emulated by the JVM, which is slower than the scalar code
        if (VectorShape.preferredShape().vectorBitSize() < SPECIES.vectorBitSize()) {
            return false;
        }
        String arch = StandardSystemProperty.OS_ARCH.value();
        arch = arch == null ? "" : arch.toLowerCase(ENGLISH);
        // A byte comparison is a single instruction on x86 (AVX2) and on ARM (SVE), but may be emulated elsewhere
        return arch.contains("x86") || arch.contains("amd64") || arch.contains("aarch64") || arch.contains("arm");
    }

    @Override
    public int vectorLength()
    {
        return SPECIES.length();
    }

    @Override
    public long match(byte[] control, int position, byte hashPrefix)
    {
        return ByteVector.fromArray(SPECIES, control, position)
                .compare(VectorOperators.EQ, hashPrefix)
                .toLong();
    }

    @Override
    public int firstSlot(long matches)
    {
        return Long.numberOfTrailingZeros(matches);
    }
}
