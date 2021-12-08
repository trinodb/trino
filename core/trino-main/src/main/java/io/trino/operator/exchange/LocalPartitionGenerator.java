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
package io.trino.operator.exchange;

import io.airlift.slice.XxHash64;
import io.trino.operator.HashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;

import static io.trino.util.MoreMath.partitionUniformly;

public class LocalPartitionGenerator
        implements PartitionFunction
{
    private final HashGenerator hashGenerator;
    private final int partitionCount;

    public LocalPartitionGenerator(HashGenerator hashGenerator, int partitionCount)
    {
        this.hashGenerator = hashGenerator;
        this.partitionCount = partitionCount;
    }

    @Override
    public int getPartitionCount()
    {
        return partitionCount;
    }

    @Override
    public int getPartition(Page page, int position)
    {
        long rawHash = getRawHash(page, position);
        return partitionUniformly(processRawHash(rawHash), partitionCount);
    }

    public long getRawHash(Page page, int position)
    {
        return hashGenerator.hashPosition(position, page);
    }

    public int getPartition(long rawHash)
    {
        return partitionUniformly(processRawHash(rawHash), partitionCount);
    }

    private static int processRawHash(long rawHash)
    {
        // mix the bits so we don't use the same hash used to distribute between stages
        return (int) XxHash64.hash(Long.reverse(rawHash));
    }
}
