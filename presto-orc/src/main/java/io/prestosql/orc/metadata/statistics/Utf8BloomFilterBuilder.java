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

import io.airlift.slice.Slice;

public class Utf8BloomFilterBuilder
        implements BloomFilterBuilder
{
    private final BloomFilter bloomFilter;

    public Utf8BloomFilterBuilder(int expectedSize, double fpp)
    {
        bloomFilter = new BloomFilter(expectedSize, fpp);
    }

    @Override
    public BloomFilterBuilder addString(Slice val)
    {
        bloomFilter.add(val);
        return this;
    }

    @Override
    public BloomFilterBuilder addLong(long val)
    {
        bloomFilter.addLong(val);
        return this;
    }

    @Override
    public BloomFilterBuilder addDouble(double val)
    {
        bloomFilter.addDouble(val);
        return this;
    }

    @Override
    public BloomFilterBuilder addFloat(float val)
    {
        bloomFilter.addFloat(val);
        return this;
    }

    @Override
    public BloomFilter buildBloomFilter()
    {
        return bloomFilter;
    }
}
