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

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static io.trino.simd.BlockEncodingSimdSupport.determineSimdSupport;
import static io.trino.simd.SimdCapability.COMPRESS_BYTE;
import static io.trino.simd.SimdCapability.COMPRESS_INT;
import static io.trino.simd.SimdCapability.COMPRESS_LONG;
import static io.trino.simd.SimdCapability.COMPRESS_SHORT;
import static io.trino.simd.SimdCapability.EXPAND_INT;
import static io.trino.simd.SimdCapability.EXPAND_LONG;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBlockEncodingSimdSupport
{
    @Test
    void testAvx512Detection()
    {
        String osArch = "amd64";
        int vectorBitsPreferred = 256;
        // Only compress and expand int and long, no byte or short support with only avx512f
        assertThat(determineSimdSupport(osArch, vectorBitsPreferred, Set.of("avx512f")))
                .isEqualTo(new BlockEncodingSimdSupport.SimdSupport(EnumSet.of(
                        COMPRESS_INT,
                        EXPAND_INT,
                        COMPRESS_LONG,
                        EXPAND_LONG)));
        // Support compress / expand for all types with avx512vbmi2
        assertThat(determineSimdSupport(osArch, vectorBitsPreferred, Set.of("avx512f", "avx512vbmi2")))
                .isEqualTo(BlockEncodingSimdSupport.SimdSupport.ALL);
    }

    @Test
    void testGraviton3Detection()
    {
        String osArch = "aarch64";
        int vectorBitsPreferred = 256;
        Set<String> flags = Set.of("sve");
        // SVE 1 supports compress for all primitive types, but no expand intrinsics in JDK 25
        BlockEncodingSimdSupport.SimdSupport expected = new BlockEncodingSimdSupport.SimdSupport(EnumSet.of(
                COMPRESS_BYTE,
                COMPRESS_SHORT,
                COMPRESS_INT,
                COMPRESS_LONG));
        assertThat(determineSimdSupport(osArch, vectorBitsPreferred, flags)).isEqualTo(expected);
    }

    @Test
    void testGraviton4Detection()
    {
        String osArch = "aarch64";
        int vectorBitsPreferred = 128;
        Set<String> flags = Set.of("sve", "sve2");
        // SVE 2 adds expand for int; long compress/expand stay off because they are not worthwhile on 128 bit vectors
        BlockEncodingSimdSupport.SimdSupport expected = new BlockEncodingSimdSupport.SimdSupport(EnumSet.of(
                COMPRESS_BYTE,
                COMPRESS_SHORT,
                COMPRESS_INT,
                EXPAND_INT));
        assertThat(determineSimdSupport(osArch, vectorBitsPreferred, flags)).isEqualTo(expected);
    }
}
