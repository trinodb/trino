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
package io.trino.plugin.iceberg;

import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.PartitionTransforms.ValueTransform;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.PartitionSpec;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public class IcebergBucketFunction
        implements BucketFunction
{
    private final int bucketCount;

    private final List<PartitionColumn> partitionColumns;
    private final List<MethodHandle> hashCodeInvokers;

    public IcebergBucketFunction(
            TypeOperators typeOperators,
            PartitionSpec partitionSpec,
            List<IcebergColumnHandle> partitioningColumns,
            int bucketCount)
    {
        requireNonNull(partitionSpec, "partitionSpec is null");
        checkArgument(!partitionSpec.isUnpartitioned(), "empty partitionSpec");
        requireNonNull(partitioningColumns, "partitioningColumns is null");
        requireNonNull(typeOperators, "typeOperators is null");
        checkArgument(bucketCount > 0, "Invalid bucketCount: %s", bucketCount);

        this.bucketCount = bucketCount;

        Map<Integer, Integer> fieldIdToInputChannel = new HashMap<>();
        for (int i = 0; i < partitioningColumns.size(); i++) {
            Integer previous = fieldIdToInputChannel.put(partitioningColumns.get(i).getId(), i);
            checkState(previous == null, "Duplicate id %s in %s at %s and %s", partitioningColumns.get(i).getId(), partitioningColumns, i, previous);
        }
        partitionColumns = partitionSpec.fields().stream()
                .map(field -> {
                    Integer channel = fieldIdToInputChannel.get(field.sourceId());
                    checkArgument(channel != null, "partition field not found: %s", field);
                    Type inputType = partitioningColumns.get(channel).getType();
                    ColumnTransform transform = getColumnTransform(field, inputType);
                    return new PartitionColumn(channel, transform.getValueTransform(), transform.getType());
                })
                .collect(toImmutableList());
        hashCodeInvokers = partitionColumns.stream()
                .map(PartitionColumn::getResultType)
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)))
                .collect(toImmutableList());
    }

    @Override
    public int getBucket(Page page, int position)
    {
        int hash = 0;

        for (int channel = 0; channel < partitionColumns.size(); channel++) {
            PartitionColumn partitionColumn = partitionColumns.get(channel);
            Block block = page.getBlock(partitionColumn.getSourceChannel());
            Object value = partitionColumn.getValueTransform().apply(block, position);
            long valueHash = hashValue(hashCodeInvokers.get(channel), value);
            hash = (31 * hash) + Long.hashCode(valueHash);
        }

        return (hash & Integer.MAX_VALUE) % bucketCount;
    }

    @Override
    public void getBuckets(Page page, int positionOffset, int length, int[] buckets)
    {
        Arrays.fill(buckets, 0, length, 0);

        for (int channel = 0; channel < partitionColumns.size(); channel++) {
            PartitionColumn partitionColumn = partitionColumns.get(channel);
            Block block = page.getBlock(partitionColumn.getSourceChannel());
            ValueTransform valueTransform = partitionColumn.getValueTransform();
            MethodHandle hashCodeInvoker = hashCodeInvokers.get(channel);
            for (int i = 0; i < length; i++) {
                int position = positionOffset + i;
                Object value = valueTransform.apply(block, position);
                long valueHash = hashValue(hashCodeInvoker, value);
                buckets[i] = (31 * buckets[i]) + Long.hashCode(valueHash);
            }
        }

        for (int i = 0; i < length; i++) {
            buckets[i] = (buckets[i] & Integer.MAX_VALUE) % bucketCount;
        }
    }

    private static long hashValue(MethodHandle method, Object value)
    {
        if (value == null) {
            return NULL_HASH_CODE;
        }
        try {
            return (long) method.invoke(value);
        }
        catch (Throwable throwable) {
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new RuntimeException(throwable);
        }
    }

    private static class PartitionColumn
    {
        private final int sourceChannel;
        private final ValueTransform valueTransform;
        private final Type resultType;

        public PartitionColumn(int sourceChannel, ValueTransform valueTransform, Type resultType)
        {
            this.sourceChannel = sourceChannel;
            this.valueTransform = requireNonNull(valueTransform, "valueTransform is null");
            this.resultType = requireNonNull(resultType, "resultType is null");
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }

        public Type getResultType()
        {
            return resultType;
        }

        public ValueTransform getValueTransform()
        {
            return valueTransform;
        }
    }
}
