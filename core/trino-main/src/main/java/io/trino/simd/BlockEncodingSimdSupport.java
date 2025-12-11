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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.StandardSystemProperty;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.FeaturesConfig;
import jdk.incubator.vector.VectorShape;

import java.util.EnumSet;
import java.util.Set;

import static io.trino.util.MachineInfo.readCpuFlags;
import static java.util.Locale.ENGLISH;

/*
We need to specifically detect AVX512F (for VPCOMPRESSD / VPCOMPRESSQ instruction support for int and long types) and
AVX512VBMI2 (for VPCOMPRESSB / VPCOMPRESSW instruction support over byte and short types). Because we would like to check
whether Vector<T>#compress(VectorMask<T>) is supported natively in hardware or emulated by the JVM - because the emulated
support is so much slower than the simple scalar code that exists, but since we don't have the ability to detect that
directly from the JDK vector API we have to assume that native support exists whenever the CPU advertises it.

On ARM, the situation is similar but the threshold for useful performance is different: NEON does not provide a native
equivalent for the vector compress operation and JVM emulation tends to be significantly slower than our scalar fallback.
SVE, on the other hand, offers predicate-driven operations that can lower Vector<T>#compress to efficient native
instructions. Since the JDK vector API does not expose whether compress is lowered to SVE or emulated, we conservatively
enable SIMD-only paths on ARM when the CPU advertises SVE (and the preferred vector width is large enough), and treat
NEON-only systems as not supporting native compress for the purposes of this optimization. In AWS Graviton families,
Graviton2 is NEON-only (no SVE), whereas Graviton3 provides SVE.
 */
@Singleton
public final class BlockEncodingSimdSupport
{
    public record SimdSupport(
            boolean compressByte,
            boolean expandByte,
            boolean compressShort,
            boolean expandShort,
            boolean compressInt,
            boolean expandInt,
            boolean compressLong,
            boolean expandLong)
    {
        public static final SimdSupport NONE = new SimdSupport(false, false, false, false, false, false, false, false);
        public static final SimdSupport ALL = new SimdSupport(true, true, true, true, true, true, true, true);
    }

    private static final int MINIMUM_SIMD_LENGTH = 256;
    private static final SimdSupport AUTO_DETECTED_SUPPORT = detectSimd();

    private final SimdSupport simdSupport;

    @Inject
    public BlockEncodingSimdSupport(FeaturesConfig featuresConfig)
    {
        this(featuresConfig.isExchangeVectorizedSerdeEnabled());
    }

    public BlockEncodingSimdSupport(boolean enableAutoDetectedSimdSupport)
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
        String archRaw = StandardSystemProperty.OS_ARCH.value();
        String arch = archRaw == null ? "" : archRaw.toLowerCase(ENGLISH);
        int preferredBitWidth = VectorShape.preferredShape().vectorBitSize();
        return determineSimdSupport(arch, preferredBitWidth, readCpuFlags());
    }

    @VisibleForTesting
    static SimdSupport determineSimdSupport(String osArch, int preferredVectorBitWidth, Set<String> flags)
    {
        if (isX86Arch(osArch)) {
            return detectX86SimdSupport(preferredVectorBitWidth, flags);
        }
        else if (isArmArch(osArch)) {
            return detectArmSimdSupport(preferredVectorBitWidth, flags);
        }
        else {
            return SimdSupport.NONE;
        }
    }

    private static boolean isX86Arch(String arch)
    {
        return arch.contains("x86") || arch.contains("amd64");
    }

    private static boolean isArmArch(String arch)
    {
        return arch.contains("arm") || arch.contains("aarch64");
    }

    private static SimdSupport detectX86SimdSupport(int preferredVectorBitWidth, Set<String> flags)
    {
        enum X86SimdInstructionSet {
            avx512f,
            avx512vbmi2
        }

        EnumSet<X86SimdInstructionSet> x86Flags = EnumSet.noneOf(X86SimdInstructionSet.class);

        if (!flags.isEmpty()) {
            for (X86SimdInstructionSet instructionSet : X86SimdInstructionSet.values()) {
                if (flags.contains(instructionSet.name())) {
                    x86Flags.add(instructionSet);
                }
            }
        }

        if (preferredVectorBitWidth < MINIMUM_SIMD_LENGTH) {
            return SimdSupport.NONE;
        }

        boolean expandAndCompressByte = x86Flags.contains(X86SimdInstructionSet.avx512vbmi2);
        boolean expandAndCompressShort = x86Flags.contains(X86SimdInstructionSet.avx512vbmi2);
        boolean expandAndCompressInt = x86Flags.contains(X86SimdInstructionSet.avx512f);
        boolean expandAndCompressLong = x86Flags.contains(X86SimdInstructionSet.avx512f);
        return new SimdSupport(
                expandAndCompressByte,
                expandAndCompressByte,
                expandAndCompressShort,
                expandAndCompressShort,
                expandAndCompressInt,
                expandAndCompressInt,
                expandAndCompressLong,
                expandAndCompressLong);
    }

    private static SimdSupport detectArmSimdSupport(int preferredVectorBitWidth, Set<String> flags)
    {
        enum ArmSimdInstructionSet {
            sve,
            sve2
        }

        EnumSet<ArmSimdInstructionSet> armFlags = EnumSet.noneOf(ArmSimdInstructionSet.class);

        if (!flags.isEmpty()) {
            for (ArmSimdInstructionSet instructionSet : ArmSimdInstructionSet.values()) {
                if (flags.contains(instructionSet.name())) {
                    armFlags.add(instructionSet);
                }
            }
        }

        // NEON support is often too slow to make this worthwhile
        if (!armFlags.contains(ArmSimdInstructionSet.sve)) {
            return SimdSupport.NONE;
        }

        // SVE 1 is sufficient to have Vector#compress(VectorMask) intrinsics for all primitive types
        boolean compressAll = true;
        boolean compressLong = preferredVectorBitWidth > 128; // only vectorize long compression if we can handle more than 2 values per instruction

        // As of JDK 25, SVE 2 intrinsics for Vector#expand(VectorMask) over int and long, but not byte or short. JDK 26 add intrinsics
        // for byte and short on SVE 2, SVE 1, and NEON in https://bugs.openjdk.org/browse/JDK-8363989. We can reconsider enabling vectorized
        // expansion for those types at some point in the future
        boolean expandInt = armFlags.contains(ArmSimdInstructionSet.sve2);
        boolean expandLong = armFlags.contains(ArmSimdInstructionSet.sve2) && preferredVectorBitWidth > 128; // ensure minimum register width for long
        boolean expandByteAndShort = false; // no intrinsics in JDK 25
        return new SimdSupport(
                compressAll,
                expandByteAndShort,
                compressAll,
                expandByteAndShort,
                compressAll,
                expandInt,
                compressLong,
                expandLong);
    }

    public SimdSupport getSimdSupport()
    {
        return simdSupport;
    }
}
