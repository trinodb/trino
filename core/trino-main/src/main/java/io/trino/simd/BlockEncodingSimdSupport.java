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
package io.trino.simd;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.FeaturesConfig;
import io.trino.spi.SimdSupport;
import io.trino.util.MachineInfo;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import oshi.hardware.CentralProcessor.ProcessorIdentifier;

import java.util.EnumSet;
import java.util.Set;

import static io.trino.FeaturesConfig.BlockSerdeVectorizedNullSuppressionStrategy.AUTO;
import static io.trino.FeaturesConfig.BlockSerdeVectorizedNullSuppressionStrategy.NONE;
import static io.trino.util.MachineInfo.readCpuFlags;
import static java.util.Locale.ENGLISH;

@Singleton
public final class BlockEncodingSimdSupport
{
    public static final int MINIMUM_SIMD_LENGTH = 512;
    private final SimdSupport simdSupport;

    public static final BlockEncodingSimdSupport TESTING_BLOCK_ENCODING_SIMD_SUPPORT = new BlockEncodingSimdSupport(new FeaturesConfig().setBlockSerdeVectorizedNullSuppressionStrategy(AUTO));

    @Inject
    public BlockEncodingSimdSupport(
            FeaturesConfig featuresConfig)
    {
        simdSupport = detectSimd(featuresConfig);
    }

    private static SimdSupport detectSimd(FeaturesConfig featuresConfig)
    {
        if (featuresConfig.getBlockSerdeVectorizedNullSuppressionStrategy().equals(NONE)) {
            return SimdSupport.NONE;
        }

        ProcessorIdentifier id = MachineInfo.getProcessorInfo();

        String vendor = id.getVendor().toLowerCase(ENGLISH);

        if (vendor.contains("intel") || vendor.contains("amd")) {
            return detectX86SimdSupport();
        }

        return SimdSupport.NONE;
    }

    private static SimdSupport detectX86SimdSupport()
    {
        enum X86Isa {
            avx512f,
            avx512vbmi2
        }

        Set<String> flags = readCpuFlags();
        EnumSet<X86Isa> x86Flags = EnumSet.noneOf(X86Isa.class);

        if (!flags.isEmpty()) {
            for (X86Isa isa : X86Isa.values()) {
                if (flags.contains(isa.name())) {
                    x86Flags.add(isa);
                }
            }
        }

        return new SimdSupport(
                (ByteVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86Isa.avx512vbmi2),
                (ShortVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86Isa.avx512vbmi2),
                (IntVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86Isa.avx512f),
                (LongVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86Isa.avx512f));
    }

    public SimdSupport getSimdSupport()
    {
        return simdSupport;
    }
}
