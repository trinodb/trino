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
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public record IcebergPartitioningHandle(boolean update, List<IcebergPartitionFunction> partitionFunctions)
        implements ConnectorPartitioningHandle
{
    public IcebergPartitioningHandle
    {
        partitionFunctions = ImmutableList.copyOf(requireNonNull(partitionFunctions, "partitioning is null"));
    }

    public IcebergPartitioningHandle forUpdate()
    {
        return new IcebergPartitioningHandle(true, partitionFunctions);
    }

    public static IcebergPartitioningHandle create(PartitionSpec spec, TypeManager typeManager, List<IcebergColumnHandle> partitioningColumns)
    {
        Map<Integer, List<Integer>> dataPaths = buildDataPaths(spec);
        List<IcebergPartitionFunction> partitionFields = spec.fields().stream()
                .map(field -> IcebergPartitionFunction.create(
                        field.transform().toString(),
                        dataPaths.get(field.sourceId()),
                        toTrinoType(spec.schema().findType(field.sourceId()), typeManager)))
                .collect(toImmutableList());

        return new IcebergPartitioningHandle(false, partitionFields);
    }

    /**
     * Constructs a map of field IDs to data paths.
     * The data path for root field is the ordinal position of the partition field under this root field, defined by {@link IcebergMetadata#getWriteLayout}
     * The data path for non-root nested fields is the ordinal position in its parent's nested field.
     * e.g. for a schema {f1: {f3, f4}, f2, f5}
     * when partitioned by f1.f3 and f2, the data paths are {3 : [1,0], 2 : [0]}
     * when partitioned by f1.f4 and f5, the data paths are {4 : [0, 1], 5 : [1]}
     */
    private static Map<Integer, List<Integer>> buildDataPaths(PartitionSpec spec)
    {
        Set<Integer> partitionFieldIds = spec.fields().stream().map(PartitionField::sourceId).collect(toImmutableSet());

        /*
         * In this loop, the field ID acts as a placeholder in the first position
         * Later, these placeholders will be replaced with the actual channel IDs by the order of its partitioned sub-field ID.
         */
        Map<Integer, List<Integer>> fieldInfo = new HashMap<>();
        for (Types.NestedField field : spec.schema().asStruct().fields()) {
            // Partition fields can only be nested in a struct
            if (field.type() instanceof Types.StructType nestedStruct) {
                buildDataPaths(partitionFieldIds, nestedStruct, new ArrayDeque<>(ImmutableList.of(field.fieldId())), fieldInfo);
            }
            else if (field.type().isPrimitiveType() && partitionFieldIds.contains(field.fieldId())) {
                fieldInfo.put(field.fieldId(), ImmutableList.of(field.fieldId()));
            }
        }

        /*
         * Replace the root field ID with the actual channel ID.
         * Transformation: {fieldId : rootFieldId.structOrdinalX.structOrdinalY} -> {fieldId : channel.structOrdinalX.structOrdinalY}.
         * Root field's channelId is assigned sequentially based on the key fieldId.
         */
        List<Integer> sortedFieldIds = fieldInfo.keySet().stream()
                .sorted()
                .collect(toImmutableList());

        ImmutableMap.Builder<Integer, List<Integer>> builder = ImmutableMap
                .builderWithExpectedSize(sortedFieldIds.size());

        Map<Integer, Integer> fieldChannels = new HashMap<>();
        AtomicInteger channel = new AtomicInteger();
        for (int sortedFieldId : sortedFieldIds) {
            List<Integer> dataPath = fieldInfo.get(sortedFieldId);
            int fieldChannel = fieldChannels.computeIfAbsent(dataPath.getFirst(), _ -> channel.getAndIncrement());
            List<Integer> channelDataPath = ImmutableList.<Integer>builder()
                    .add(fieldChannel)
                    .addAll(dataPath.stream()
                            .skip(1)
                            .iterator())
                    .build();
            builder.put(sortedFieldId, channelDataPath);
        }

        return builder.buildOrThrow();
    }

    private static void buildDataPaths(Set<Integer> partitionFieldIds, Types.StructType struct, ArrayDeque<Integer> currentPaths, Map<Integer, List<Integer>> dataPaths)
    {
        List<Types.NestedField> fields = struct.fields();
        for (int fieldOrdinal = 0; fieldOrdinal < fields.size(); fieldOrdinal++) {
            Types.NestedField field = fields.get(fieldOrdinal);
            int fieldId = field.fieldId();

            currentPaths.addLast(fieldOrdinal);
            org.apache.iceberg.types.Type type = field.type();
            if (type instanceof Types.StructType nestedStruct) {
                buildDataPaths(partitionFieldIds, nestedStruct, currentPaths, dataPaths);
            }
            // Map and List types are not supported in partitioning
            else if (type.isPrimitiveType() && partitionFieldIds.contains(fieldId)) {
                dataPaths.put(fieldId, ImmutableList.copyOf(currentPaths));
            }
            currentPaths.removeLast();
        }
    }

    public long getCacheKeyHint()
    {
        Hasher hasher = Hashing.goodFastHash(64).newHasher();
        hasher.putBoolean(update);
        for (IcebergPartitionFunction function : partitionFunctions) {
            hasher.putInt(function.transform().ordinal());
            function.dataPath().forEach(hasher::putInt);
            hasher.putString(function.type().getTypeSignature().toString(), UTF_8);
            function.size().ifPresent(hasher::putInt);
        }
        return hasher.hash().asLong();
    }
}
