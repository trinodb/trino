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
            boolean expandAndCompressByte,
            boolean expandAndCompressShort,
            boolean expandAndCompressInt,
            boolean expandAndCompressLong)
    {
        public static final SimdSupport NONE = new SimdSupport(false, false, false, false);
        public static final SimdSupport ALL = new SimdSupport(true, true, true, true);
    }

    private static final int MINIMUM_SIMD_LENGTH = 256;
    private static final int PREFERRED_BIT_WIDTH = VectorShape.preferredShape().vectorBitSize();
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

        if (isX86Arch(arch)) {
            return detectX86SimdSupport();
        }
        if (isArmArch(arch)) {
            return detectArmSimdSupport();
        }

        return SimdSupport.NONE;
    }

    private static boolean isX86Arch(String arch)
    {
        return arch.contains("x86") || arch.contains("amd64");
    }

    private static boolean isArmArch(String arch)
    {
        return arch.contains("arm") || arch.contains("aarch64");
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

        if (PREFERRED_BIT_WIDTH < MINIMUM_SIMD_LENGTH) {
            return SimdSupport.NONE;
        }
        else {
            return new SimdSupport(
                    x86Flags.contains(X86SimdInstructionSet.avx512vbmi2),
                    x86Flags.contains(X86SimdInstructionSet.avx512vbmi2),
                    x86Flags.contains(X86SimdInstructionSet.avx512f),
                    x86Flags.contains(X86SimdInstructionSet.avx512f));
        }
    }

    private static SimdSupport detectArmSimdSupport()
    {
        enum ArmSimdInstructionSet {
            sve,
        }

        Set<String> flags = readCpuFlags();
        EnumSet<ArmSimdInstructionSet> armFlags = EnumSet.noneOf(ArmSimdInstructionSet.class);

        if (!flags.isEmpty()) {
            for (ArmSimdInstructionSet instructionSet : ArmSimdInstructionSet.values()) {
                if (flags.contains(instructionSet.name())) {
                    armFlags.add(instructionSet);
                }
            }
        }

        if (PREFERRED_BIT_WIDTH < MINIMUM_SIMD_LENGTH || !armFlags.contains(ArmSimdInstructionSet.sve)) {
            return SimdSupport.NONE;
        }

        return SimdSupport.ALL;
    }

    public SimdSupport getSimdSupport()
    {
        return simdSupport;
    }
}
