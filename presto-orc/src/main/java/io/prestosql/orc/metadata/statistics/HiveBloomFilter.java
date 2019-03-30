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
package io.prestosql.orc.metadata.statistics;

import com.google.common.primitives.Longs;
import io.prestosql.orc.metadata.statistics.StatisticsHasher.Hashable;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;

public class HiveBloomFilter
        extends BloomFilter
        implements Hashable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveBloomFilter.class).instanceSize() + ClassLayout.parseClass(BitSet.class).instanceSize();

    public HiveBloomFilter(long expectedEntries, double fpp)
    {
        super(expectedEntries, fpp);
    }

    // constructor that allows deserialization of a long list into the actual hive bloom filter
    public HiveBloomFilter(List<Long> bits, int numHashFunctions)
    {
        super(Longs.toArray(bits), numHashFunctions);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(getBitSet());
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putInt(getNumBits())
                .putInt(getNumHashFunctions())
                .putLongs(getBitSet());
    }
}
