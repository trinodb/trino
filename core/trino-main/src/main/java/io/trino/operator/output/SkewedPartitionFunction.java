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

package io.trino.operator.output;

import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;

import static java.util.Objects.requireNonNull;

public class SkewedPartitionFunction
        implements PartitionFunction
{
    private final PartitionFunction partitionFunction;
    private final SkewedPartitionRebalancer skewedPartitionRebalancer;

    private final long[] partitionRowCount;

    public SkewedPartitionFunction(PartitionFunction partitionFunction, SkewedPartitionRebalancer skewedPartitionRebalancer)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.skewedPartitionRebalancer = requireNonNull(skewedPartitionRebalancer, "skewedPartitionRebalancer is null");

        this.partitionRowCount = new long[partitionFunction.getPartitionCount()];
    }

    @Override
    public int getPartitionCount()
    {
        return skewedPartitionRebalancer.getTaskCount();
    }

    @Override
    public int getPartition(Page page, int position)
    {
        int partition = partitionFunction.getPartition(page, position);
        return skewedPartitionRebalancer.getTaskId(partition, partitionRowCount[partition]++);
    }

    public void flushPartitionRowCountToRebalancer()
    {
        for (int partition = 0; partition < partitionFunction.getPartitionCount(); partition++) {
            skewedPartitionRebalancer.addPartitionRowCount(partition, partitionRowCount[partition]);
            partitionRowCount[partition] = 0;
        }
    }
}
