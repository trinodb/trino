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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.deflate.DeflateCompressor;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.trino.orc.metadata.CompressionKind;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static java.util.Objects.requireNonNull;

/**
 * Pool of compressors shared by all {@link OrcOutputBuffer} instances.
 * <p>
 * A writer has an output buffer per stream of every column, so a compressor per buffer
 * means hundreds of compressors for a wide table. Native compressors hold off-heap state
 * that is released only when the compressor is garbage collected (the native deflate
 * compressor holds about 650 KB), which is not visible to the memory accounting.
 * Compression is a one-shot operation, so a compressor is needed only for the duration
 * of a single call, and the number of compressors is bounded by the number of concurrent
 * compression calls rather than by the number of open streams.
 * <p>
 * Sharing relies on two properties of the compressors:
 * <ul>
 *     <li>The output of a call depends only on its input. No state from earlier calls,
 *     such as statistics gathered to tune compression, carries over, so a compressor that
 *     already compressed data of another stream produces the same output as a new one.</li>
 *     <li>A compressor is not thread-safe, so it is used by a single thread at a time:
 *     a borrowed compressor is removed from the pool until it is released.</li>
 * </ul>
 */
final class OrcCompressorPool
{
    private final Map<CompressionKind, BlockingQueue<Compressor>> pools = new EnumMap<>(CompressionKind.class);

    OrcCompressorPool(int maxPooledCompressorsPerKind)
    {
        checkArgument(maxPooledCompressorsPerKind > 0, "maxPooledCompressorsPerKind must be greater than 0");
        for (CompressionKind compression : CompressionKind.values()) {
            if (compression != NONE) {
                pools.put(compression, new ArrayBlockingQueue<>(maxPooledCompressorsPerKind));
            }
        }
    }

    public Compressor borrow(CompressionKind compression)
    {
        Compressor compressor = pool(compression).poll();
        if (compressor != null) {
            return compressor;
        }
        return createCompressor(compression);
    }

    public void release(CompressionKind compression, Compressor compressor)
    {
        requireNonNull(compressor, "compressor is null");
        // when the pool is full the compressor is dropped and released by the garbage collector
        pool(compression).offer(compressor);
    }

    @VisibleForTesting
    int getPooledCount(CompressionKind compression)
    {
        return pool(compression).size();
    }

    private BlockingQueue<Compressor> pool(CompressionKind compression)
    {
        requireNonNull(compression, "compression is null");
        checkArgument(compression != NONE, "no compressor for compression NONE");
        return pools.get(compression);
    }

    private static Compressor createCompressor(CompressionKind compression)
    {
        return switch (compression) {
            case NONE -> throw new IllegalArgumentException("no compressor for compression NONE");
            case SNAPPY -> SnappyCompressor.create();
            case ZLIB -> DeflateCompressor.create();
            case LZ4 -> Lz4Compressor.create();
            case ZSTD -> ZstdCompressor.create();
        };
    }
}
