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
package io.trino.plugin.deltalake;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;

public class DeltaLakeBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final List<MethodHandle> hashCodeInvokers;

    public DeltaLakeBucketFunction(TypeOperators typeOperators, List<DeltaLakeColumnHandle> partitioningColumns, int bucketCount)
    {
        this.hashCodeInvokers = partitioningColumns.stream()
                .map(DeltaLakeColumnHandle::getType)
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)))
                .collect(toImmutableList());
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        int hash = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            long valueHash = hashValue(hashCodeInvokers.get(channel), block, position);
            hash = (31 * hash) + Long.hashCode(valueHash);
        }
        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    @Override
    public void getBuckets(Page page, int positionOffset, int length, int[] buckets)
    {
        Arrays.fill(buckets, 0, length, 0);
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            MethodHandle hashCodeInvoker = hashCodeInvokers.get(channel);
            for (int i = 0; i < length; i++) {
                int position = positionOffset + i;
                long valueHash = hashValue(hashCodeInvoker, block, position);
                buckets[i] = (31 * buckets[i]) + Long.hashCode(valueHash);
            }
        }
        for (int i = 0; i < length; i++) {
            buckets[i] = (buckets[i] & Integer.MAX_VALUE) % bucketCount;
        }
    }

    private static long hashValue(MethodHandle method, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        try {
            return (long) method.invokeExact(block, position);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }
}
