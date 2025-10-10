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
package io.trino.spi.simd;

import io.trino.spi.SimdSupport;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;

import java.util.Set;

import static io.trino.spi.simd.SimdUtils.MINIMUM_SIMD_LENGTH;
import static io.trino.spi.simd.SimdUtils.normalizeFlag;

public final class AmdSimdSupport
        implements SimdSupport
{
    private enum Isa {
        AVX2,
        AVX512F,
        AVX512VBMI2
    }

    private final boolean[] has = new boolean[Isa.values().length];

    public AmdSimdSupport(OSType osType)
    {
        Set<String> flags = SimdUtils.readCpuFlags(osType);
        if (!flags.isEmpty()) {
            for (Isa isa : Isa.values()) {
                String token = normalizeFlag(isa.name());
                setIf(flags, token, isa);
            }
        }
    }

    private void setIf(Set<String> flags, String flag, Isa isa)
    {
        if (flags.contains(flag)) {
            has[isa.ordinal()] = true;
        }
    }

    @Override
    public boolean supportByteGeneric()
    {
        return (ByteVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && has[Isa.AVX512F.ordinal()];
    }

    @Override
    public boolean supportShortGeneric()
    {
        return (ShortVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && has[Isa.AVX512F.ordinal()];
    }

    @Override
    public boolean supportIntegerGeneric()
    {
        return (IntVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && has[Isa.AVX512F.ordinal()];
    }

    @Override
    public boolean supportLongGeneric()
    {
        return (LongVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && has[Isa.AVX512F.ordinal()];
    }

    @Override
    public boolean supportByteCompress()
    {
        return has[Isa.AVX512VBMI2.ordinal()];
    }

    @Override
    public boolean supportShortCompress()
    {
        return has[Isa.AVX512VBMI2.ordinal()];
    }

    @Override
    public boolean supportIntegerCompress()
    {
        return has[Isa.AVX512F.ordinal()];
    }

    @Override
    public boolean supportLongCompress()
    {
        return has[Isa.AVX512F.ordinal()];
    }
}
