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
package io.trino.plugin.hive;

import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class HivePartitionedBucketFunction
        implements BucketFunction
{
    private final BucketingVersion bucketingVersion;
    private final int hiveBucketCount;
    private final List<TypeInfo> bucketTypeInfos;
    private final int bucketCount;
    private final int firstPartitionColumnIndex;
    private final List<MethodHandle> hashCodeInvokers;

    public HivePartitionedBucketFunction(
            BucketingVersion bucketingVersion,
            int hiveBucketCount,
            List<HiveType> hiveBucketTypes,
            List<Type> partitionColumnsTypes,
            TypeOperators typeOperators,
            int bucketCount)
    {
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.hiveBucketCount = hiveBucketCount;
        this.bucketTypeInfos = hiveBucketTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());
        this.firstPartitionColumnIndex = hiveBucketTypes.size();
        this.hashCodeInvokers = partitionColumnsTypes.stream()
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)))
                .collect(toImmutableList());
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        long partitionHash = 0;
        for (int i = 0; i < hashCodeInvokers.size(); i++) {
            try {
                Block partitionColumn = page.getBlock(i + firstPartitionColumnIndex);
                partitionHash = (31 * partitionHash) + hashCodeNullSafe(hashCodeInvokers.get(i), partitionColumn, position);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }

        int hiveBucket = HiveBucketing.getHiveBucket(bucketingVersion, hiveBucketCount, bucketTypeInfos, page, position);

        return (int) ((((31 * partitionHash) + hiveBucket) & Long.MAX_VALUE) % bucketCount);
    }

    private static long hashCodeNullSafe(MethodHandle hashCode, Block block, int position)
            throws Throwable
    {
        if (block.isNull(position)) {
            // use -1 as a hash for null value as it's less likely to collide with
            // hash for non-null values (mainly 0 bigints/integers)
            return -1;
        }
        return (long) hashCode.invokeExact(block, position);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", bucketingVersion)
                .add("hiveBucketCount", hiveBucketCount)
                .add("bucketTypeInfos", bucketTypeInfos)
                .add("firstPartitionColumnIndex", firstPartitionColumnIndex)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
