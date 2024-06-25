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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.PartitionTransforms.ValueTransform;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.PartitionSpec;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.String.format;
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

        Map<String, FieldInfo> nameToFieldInfo = buildNameToFieldInfo(partitioningColumns);
        partitionColumns = partitionSpec.fields().stream()
                .map(field -> {
                    String fieldName = partitionSpec.schema().findColumnName(field.sourceId());
                    FieldInfo fieldInfo = nameToFieldInfo.get(fieldName);
                    checkArgument(fieldInfo != null, "partition field not found: %s", field);
                    ColumnTransform transform = getColumnTransform(field, fieldInfo.type());
                    return new PartitionColumn(fieldInfo.sourceChannel(), transform.getValueTransform(), transform.getType(), fieldInfo.path());
                })
                .collect(toImmutableList());
        hashCodeInvokers = partitionColumns.stream()
                .map(PartitionColumn::resultType)
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)))
                .collect(toImmutableList());
    }

    private static void addFieldInfo(
            Map<String, FieldInfo> nameToFieldInfo,
            int sourceChannel,
            String fieldName,
            List<ColumnIdentity> children,
            Type type,
            List<Integer> path)
    {
        if (type instanceof RowType rowType) {
            List<RowType.Field> fields = rowType.getFields();
            checkArgument(children.size() == fields.size(), format("children size (%s) == fields size (%s) is not equal", children.size(), fields.size()));
            for (int i = 0; i < fields.size(); i++) {
                ColumnIdentity child = children.get(i);
                String qualifiedFieldName = fieldName + "." + child.getName();
                path.add(i);
                if (fields.get(i).getType() instanceof RowType) {
                    addFieldInfo(nameToFieldInfo, sourceChannel, qualifiedFieldName, child.getChildren(), fields.get(i).getType(), path);
                }
                else {
                    nameToFieldInfo.put(qualifiedFieldName, new FieldInfo(sourceChannel, qualifiedFieldName, fields.get(i).getType(), ImmutableList.copyOf(path)));
                }
                path.removeLast();
            }
        }
        else {
            nameToFieldInfo.put(fieldName, new FieldInfo(sourceChannel, fieldName, type, ImmutableList.copyOf(path)));
        }
    }

    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;

        for (int i = 0; i < partitionColumns.size(); i++) {
            PartitionColumn partitionColumn = partitionColumns.get(i);
            Block block = page.getBlock(partitionColumn.sourceChannel());
            for (int index : partitionColumn.path()) {
                block = getRowFieldsFromBlock(block).get(index);
            }
            Object value = partitionColumn.valueTransform().apply(block, position);
            long valueHash = hashValue(hashCodeInvokers.get(i), value);
            hash = (31 * hash) + valueHash;
        }

        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    private static Map<String, FieldInfo> buildNameToFieldInfo(List<IcebergColumnHandle> partitioningColumns)
    {
        Map<String, FieldInfo> nameToFieldInfo = new HashMap<>();
        for (int channel = 0; channel < partitioningColumns.size(); channel++) {
            IcebergColumnHandle partitionColumn = partitioningColumns.get(channel);
            addFieldInfo(
                    nameToFieldInfo,
                    channel,
                    partitionColumn.getName(),
                    partitionColumn.getColumnIdentity().getChildren(),
                    partitionColumn.getType(),
                    new LinkedList<>());
        }
        return nameToFieldInfo;
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

    private record PartitionColumn(int sourceChannel, ValueTransform valueTransform, Type resultType, List<Integer> path)
    {
        private PartitionColumn
        {
            requireNonNull(valueTransform, "valueTransform is null");
            requireNonNull(resultType, "resultType is null");
            path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        }
    }

    private record FieldInfo(int sourceChannel, String name, Type type, List<Integer> path)
    {
        FieldInfo
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        }
    }
}
