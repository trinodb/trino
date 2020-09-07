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
package io.prestosql.spi.type;

import io.airlift.slice.XxHash64;
import io.prestosql.spi.block.BlockBuilder;

import static java.lang.Long.rotateLeft;

public final class TimestampTypes
{
    private TimestampTypes() {}

    public static long hashShortTimestamp(long value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }

    public static long hashLongTimestamp(LongTimestamp value)
    {
        return hashLongTimestamp(value.getEpochMicros(), value.getPicosOfMicro());
    }

    static long hashLongTimestamp(long epochMicros, long fraction)
    {
        return XxHash64.hash(epochMicros) ^ XxHash64.hash(fraction);
    }

    public static void writeLongTimestamp(BlockBuilder blockBuilder, LongTimestamp timestamp)
    {
        writeLongTimestamp(blockBuilder, timestamp.getEpochMicros(), timestamp.getPicosOfMicro());
    }

    public static void writeLongTimestamp(BlockBuilder blockBuilder, long epochMicros, int fraction)
    {
        blockBuilder.writeLong(epochMicros);
        blockBuilder.writeInt(fraction);
        blockBuilder.closeEntry();
    }
}
