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
package io.trino.orc;

import io.airlift.compress.v3.Compressor;
import io.trino.orc.metadata.CompressionKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static io.trino.orc.metadata.CompressionKind.LZ4;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.CompressionKind.ZLIB;
import static io.trino.orc.metadata.CompressionKind.ZSTD;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOrcCompressorPool
{
    @Test
    public void testReusesReleasedCompressor()
    {
        OrcCompressorPool pool = new OrcCompressorPool(2);
        for (CompressionKind compression : CompressionKind.values()) {
            if (compression == NONE) {
                continue;
            }
            Compressor compressor = pool.borrow(compression);
            assertThat(pool.getPooledCount(compression)).isEqualTo(0);
            pool.release(compression, compressor);
            assertThat(pool.getPooledCount(compression)).isEqualTo(1);
            assertThat(pool.borrow(compression)).isSameAs(compressor);
            assertThat(pool.getPooledCount(compression)).isEqualTo(0);
        }
    }

    @Test
    public void testReusedCompressorProducesSameOutputAsNewCompressor()
    {
        byte[] previous = generateData(1, 200_000);
        byte[] current = generateData(2, 200_000);

        for (CompressionKind compression : CompressionKind.values()) {
            if (compression == NONE) {
                continue;
            }
            byte[] expected = compress(new OrcCompressorPool(1).borrow(compression), current);

            OrcCompressorPool pool = new OrcCompressorPool(1);
            Compressor compressor = pool.borrow(compression);
            compress(compressor, previous);
            pool.release(compression, compressor);
            Compressor reused = pool.borrow(compression);
            assertThat(reused).isSameAs(compressor);

            assertThat(compress(reused, current))
                    .describedAs("output of a reused %s compressor", compression)
                    .isEqualTo(expected);
        }
    }

    @Test
    public void testConcurrentBorrowsGetDistinctCompressors()
    {
        OrcCompressorPool pool = new OrcCompressorPool(2);
        Compressor first = pool.borrow(ZLIB);
        Compressor second = pool.borrow(ZLIB);
        assertThat(second).isNotSameAs(first);
        pool.release(ZLIB, first);
        pool.release(ZLIB, second);
        assertThat(pool.getPooledCount(ZLIB)).isEqualTo(2);
    }

    @Test
    public void testPoolSizeIsBounded()
    {
        OrcCompressorPool pool = new OrcCompressorPool(1);
        Compressor first = pool.borrow(ZSTD);
        Compressor second = pool.borrow(ZSTD);
        pool.release(ZSTD, first);
        pool.release(ZSTD, second);
        assertThat(pool.getPooledCount(ZSTD)).isEqualTo(1);
        assertThat(pool.borrow(ZSTD)).isSameAs(first);
    }

    @Test
    public void testPoolsAreSeparatePerCompressionKind()
    {
        OrcCompressorPool pool = new OrcCompressorPool(2);
        pool.release(LZ4, pool.borrow(LZ4));
        assertThat(pool.getPooledCount(LZ4)).isEqualTo(1);
        assertThat(pool.getPooledCount(ZLIB)).isEqualTo(0);
    }

    @Test
    public void testNoCompressorForNone()
    {
        OrcCompressorPool pool = new OrcCompressorPool(2);
        assertThatThrownBy(() -> pool.borrow(NONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no compressor for compression NONE");
    }

    private static byte[] compress(Compressor compressor, byte[] input)
    {
        byte[] output = new byte[compressor.maxCompressedLength(input.length)];
        int compressedSize = compressor.compress(input, 0, input.length, output, 0, output.length);
        return Arrays.copyOf(output, compressedSize);
    }

    // compressible data with a seed-dependent vocabulary, so different seeds give differently distributed symbols
    private static byte[] generateData(long seed, int size)
    {
        Random random = new Random(seed);
        String[] words = new String[64];
        for (int i = 0; i < words.length; i++) {
            StringBuilder word = new StringBuilder();
            int length = 3 + random.nextInt(10);
            for (int j = 0; j < length; j++) {
                word.append((char) ('a' + random.nextInt(26)));
            }
            words[i] = word.toString();
        }
        StringBuilder data = new StringBuilder(size);
        while (data.length() < size) {
            data.append(words[(int) Math.abs(random.nextGaussian() * words.length / 4) % words.length]).append(' ');
        }
        return data.substring(0, size).getBytes(UTF_8);
    }
}
