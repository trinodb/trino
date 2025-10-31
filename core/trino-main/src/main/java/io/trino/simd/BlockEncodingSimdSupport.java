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
import io.trino.util.MachineInfo;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import oshi.hardware.CentralProcessor.ProcessorIdentifier;

import java.util.EnumSet;
import java.util.Set;

import static io.trino.FeaturesConfig.BlockSerdeVectorizedNullSuppressionStrategy.AUTO;
import static io.trino.util.MachineInfo.readCpuFlags;
import static java.util.Locale.ENGLISH;

/*
We need to specifically detect AVX512F (for VPCOMPRESSD / VPCOMPRESSQ instruction support for int and long types) and
AVX512VBMI2 (for VPCOMPRESSB / VPCOMPRESSW instruction support over byte and short types). Because we would like to check
whether Vector<T>#compress(VectorMask<T>) is supported natively in hardware or emulated by the JVM - because the emulated
support is so much slower than the simple scalar code that exists, but since we don't have the ability to detect that
directly from the JDK vector API we have to assume that native support exists whenever the CPU advertises it.
 */
@Singleton
public final class BlockEncodingSimdSupport
{
    public record SimdSupport(
            boolean expandAndCompressByte,
            boolean expandAndCompressShort,
            boolean expandAndCompressInt,
            boolean expandAndCompressLong)
    {
        public static final SimdSupport NONE = new SimdSupport(false, false, false, false);
        public static final SimdSupport ALL = new SimdSupport(true, true, true, true);
    }

    public static final int MINIMUM_SIMD_LENGTH = 512;
    private final SimdSupport simdSupport;
    private static final SimdSupport AUTO_DETECTED_SUPPORT = detectSimd();

    public static final BlockEncodingSimdSupport TESTING_BLOCK_ENCODING_SIMD_SUPPORT = new BlockEncodingSimdSupport(true);

    @Inject
    public BlockEncodingSimdSupport(
            FeaturesConfig featuresConfig)
    {
        this(featuresConfig.getBlockSerdeVectorizedNullSuppressionStrategy().equals(AUTO));
    }

    public BlockEncodingSimdSupport(
            boolean enableAutoDetectedSimdSupport)
    {
        if (enableAutoDetectedSimdSupport) {
            simdSupport = AUTO_DETECTED_SUPPORT;
        }
        else {
            simdSupport = SimdSupport.NONE;
        }
    }

    private static SimdSupport detectSimd()
    {
        ProcessorIdentifier id = MachineInfo.getProcessorInfo();

        String vendor = id.getVendor().toLowerCase(ENGLISH);

        if (vendor.contains("intel") || vendor.contains("amd")) {
            return detectX86SimdSupport();
        }

        return SimdSupport.NONE;
    }

    private static SimdSupport detectX86SimdSupport()
    {
        enum X86SimdInstructionSet {
            avx512f,
            avx512vbmi2
        }

        Set<String> flags = readCpuFlags();
        EnumSet<X86SimdInstructionSet> x86Flags = EnumSet.noneOf(X86SimdInstructionSet.class);

        if (!flags.isEmpty()) {
            for (X86SimdInstructionSet instructionSet : X86SimdInstructionSet.values()) {
                if (flags.contains(instructionSet.name())) {
                    x86Flags.add(instructionSet);
                }
            }
        }

        return new SimdSupport(
                (ByteVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86SimdInstructionSet.avx512vbmi2),
                (ShortVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86SimdInstructionSet.avx512vbmi2),
                (IntVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86SimdInstructionSet.avx512f),
                (LongVector.SPECIES_PREFERRED.vectorBitSize() >= MINIMUM_SIMD_LENGTH) && x86Flags.contains(X86SimdInstructionSet.avx512f));
    }

    public SimdSupport getSimdSupport()
    {
        return simdSupport;
    }
}
