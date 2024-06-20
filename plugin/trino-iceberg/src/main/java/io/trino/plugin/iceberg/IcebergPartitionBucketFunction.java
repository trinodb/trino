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

import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergBucketFunction.hashValue;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionField;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class IcebergPartitionBucketFunction
{
    private final int partitionSpecId;
    private final org.apache.iceberg.types.Type[] types;
    private final int[] fieldIdx;
    private final MethodHandle[] hashCodeInvokers;
    private final List<Integer> bucketCounts;
    private final int totalBucketCount;

    public IcebergPartitionBucketFunction(Schema schema,
            TypeManager typeManager,
            List<String> partitioning,
            List<IcebergColumnHandle> partitioningColumns,
            PartitionSpec partitionSpec,
            List<Integer> bucketCounts)
    {
        partitionSpecId = partitionSpec.specId();
        checkArgument(partitioning.size() == partitioningColumns.size(), "partitioning and partitioningColumns size does not match");
        types = new Type[partitioningColumns.size()];
        fieldIdx = new int[partitioningColumns.size()];
        hashCodeInvokers = new MethodHandle[partitioningColumns.size()];
        Map<Integer, Integer> idToIdx = new HashMap<>();
        Map<Integer, PartitionField> idToPartitionField = new HashMap<>();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (!partitioning.contains(toPartitionField(partitionSpec, field))) {
                continue;
            }
            idToIdx.put(field.sourceId(), i);
            idToPartitionField.put(field.sourceId(), field);
        }
        for (int i = 0; i < partitioningColumns.size(); i++) {
            int sourceId = partitioningColumns.get(i).getBaseColumn().getId();
            PartitionField field = idToPartitionField.get(sourceId);
            types[i] = field.transform().getResultType(schema.findType(field.sourceId()));
            fieldIdx[i] = idToIdx.get(sourceId);
            hashCodeInvokers[i] = typeManager.getTypeOperators().getHashCodeOperator(toTrinoType(types[i], typeManager), simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        }
        checkArgument(bucketCounts.size() == partitioning.size(), "bucketCounts and partitioning size does not match");
        checkArgument(bucketCounts.stream().allMatch(x -> x > 0), "bucketCounts cannot have <=0 value");
        this.bucketCounts = requireNonNull(bucketCounts, "bucketCounts");
        this.totalBucketCount = bucketCounts.stream().reduce(1, (x, y) -> x * y);
    }

    public int getBucket(StructLike partitionData, int specId)
    {
        checkArgument(this.partitionSpecId == specId, "Reading file that have different specId");
        long hash = 0;
        for (int i = 0; i < fieldIdx.length; i++) {
            int idx = fieldIdx[i];
            // BucketTransform should always provide Integer result value. If we are to include other transform types, we should expand here.
            long value = (long) convertIcebergValueToTrino(types[i], partitionData.get(idx, types[i].typeId().javaClass()));
            long valueHash = hashValue(hashCodeInvokers[i], value % bucketCounts.get(i));
            hash = (31 * hash) + valueHash;
        }

        return (int) ((hash & Long.MAX_VALUE) % totalBucketCount);
    }
}
