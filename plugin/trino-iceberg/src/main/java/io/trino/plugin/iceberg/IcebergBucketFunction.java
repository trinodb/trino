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

import io.trino.plugin.iceberg.PartitionTransforms.ValueTransform;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergPartitionFunction.Transform.BUCKET;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class IcebergBucketFunction
        implements BucketFunction, ToIntFunction<ConnectorSplit>
{
    private final int bucketCount;
    private final List<HashFunction> functions;

    private final boolean singleBucketFunction;

    public IcebergBucketFunction(IcebergPartitioningHandle partitioningHandle, TypeOperators typeOperators, int bucketCount)
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        requireNonNull(typeOperators, "typeOperators is null");
        checkArgument(bucketCount > 0, "Invalid bucketCount: %s", bucketCount);

        this.bucketCount = bucketCount;
        List<IcebergPartitionFunction> partitionFunctions = partitioningHandle.partitionFunctions();
        this.functions = partitionFunctions.stream()
                .map(partitionFunction -> HashFunction.create(partitionFunction, typeOperators))
                .collect(toImmutableList());

        this.singleBucketFunction = partitionFunctions.size() == 1 &&
                partitionFunctions.getFirst().transform() == BUCKET &&
                partitionFunctions.getFirst().size().orElseThrow() == bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        if (singleBucketFunction) {
            long bucket = (long) requireNonNullElse(functions.getFirst().getValue(page, position), 0L);
            checkArgument(0 <= bucket && bucket < bucketCount, "Bucket value out of range: %s (bucketCount: %s)", bucket, bucketCount);
            return (int) bucket;
        }

        long hash = 0;
        for (HashFunction function : functions) {
            long valueHash = function.computeHash(page, position);
            hash = (31 * hash) + valueHash;
        }

        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    @Override
    public int applyAsInt(ConnectorSplit split)
    {
        List<Object> partitionValues = ((IcebergSplit) split).getPartitionValues()
                .orElseThrow(() -> new IllegalArgumentException("Split does not contain partition values"));

        if (singleBucketFunction) {
            long bucket = (long) requireNonNullElse(partitionValues.getFirst(), 0);
            checkArgument(0 <= bucket && bucket < bucketCount, "Bucket value out of range: %s (bucketCount: %s)", bucket, bucketCount);
            return (int) bucket;
        }

        long hash = 0;
        for (int i = 0; i < functions.size(); i++) {
            long valueHash = functions.get(i).computeHash(partitionValues.get(i));
            hash = (31 * hash) + valueHash;
        }

        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    private record HashFunction(List<Integer> dataPath, ValueTransform valueTransform, MethodHandle hashCodeOperator)
    {
        private static HashFunction create(IcebergPartitionFunction partitionFunction, TypeOperators typeOperators)
        {
            PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(partitionFunction);
            return new HashFunction(
                    partitionFunction.dataPath(),
                    columnTransform.valueTransform(),
                    typeOperators.getHashCodeOperator(columnTransform.type(), simpleConvention(FAIL_ON_NULL, NEVER_NULL)));
        }

        private HashFunction
        {
            requireNonNull(valueTransform, "valueTransform is null");
            requireNonNull(hashCodeOperator, "hashCodeOperator is null");
        }

        public Object getValue(Page page, int position)
        {
            Block block = page.getBlock(dataPath.getFirst());
            for (int i = 1; i < dataPath.size(); i++) {
                position = block.getUnderlyingValuePosition(position);
                block = ((RowBlock) block.getUnderlyingValueBlock()).getFieldBlock(dataPath.get(i));
            }
            return valueTransform.apply(block, position);
        }

        public long computeHash(Page page, int position)
        {
            return computeHash(getValue(page, position));
        }

        private long computeHash(Object value)
        {
            if (value == null) {
                return NULL_HASH_CODE;
            }
            try {
                return (long) hashCodeOperator.invoke(value);
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
    }
}
