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
package io.trino.plugin.hidden.partitioning;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionResult;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningExpressionConverter.toIcebergExpression;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningTypeConverter.toIcebergType;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.stream.Collectors.toMap;

public final class HiddenPartitioningUtil
{
    private HiddenPartitioningUtil() {}

    public static org.apache.iceberg.PartitionSpec toIcebergPartitionSpec(ConnectorTableMetadata tableMetadata, HiddenPartitioningPartitionSpec hiddenPartitioningPartitionSpec)
    {
        Schema icebergSchema = toIcebergSchema(tableMetadata.getColumns());
        return HiddenPartitioningSpecParser.toIcebergPartitionSpec(hiddenPartitioningPartitionSpec, icebergSchema);
    }

    public static HivePartitionResult transformFilteredPartitionResult(HivePartitionResult partitionResult, org.apache.iceberg.PartitionSpec icebergPartitionSpec)
    {
        List<HiveColumnHandle> partitionColumns = partitionResult.getPartitionColumns();
        TupleDomain<ColumnHandle> unenforcedConstraint = partitionResult.getEffectivePredicate().filter((column, domain) -> !partitionColumns.contains(column));
        ManifestEvaluator manifestEvaluator = getManifestEvaluator(unenforcedConstraint, icebergPartitionSpec);

        Iterable<HivePartition> filteredPartitions = () -> stream(partitionResult.getPartitions())
                .filter(partition -> transformedPartitionMatches(partitionResult.getPartitionColumns(), partition, manifestEvaluator, icebergPartitionSpec))
                .iterator();

        return new HivePartitionResult(
                partitionResult.getPartitionColumns(),
                partitionResult.getPartitionNames(),
                filteredPartitions,
                partitionResult.getEffectivePredicate(),
                partitionResult.getCompactEffectivePredicate(),
                partitionResult.getBucketHandle(),
                partitionResult.getBucketFilter());
    }

    private static boolean transformedPartitionMatches(
            List<HiveColumnHandle> partitionColumns,
            HivePartition partition,
            ManifestEvaluator evaluator,
            org.apache.iceberg.PartitionSpec icebergPartitionSpec)
    {
        Map<ColumnHandle, NullableValue> keyVals = partition.getKeys();
        Map<String, NullableValue> valueMap = partitionColumns.stream().collect(toMap(HiveColumnHandle::getName, keyVals::get));

        ImmutableList.Builder<ManifestFile.PartitionFieldSummary> fieldSummaryList = ImmutableList.builder();
        for (PartitionField specField : icebergPartitionSpec.fields()) {
            NullableValue partitionValue = requireNonNull(valueMap.get(specField.name()), "Didn't find partition value for field: " + specField.name());
            ByteBuffer value = toIcebergValue(partitionValue);
            fieldSummaryList.add(new HiddenPartitioningManifestFile.TrinoPartitionFieldSummary(partitionValue.isNull(), value, value));
        }
        return evaluator.eval(new HiddenPartitioningManifestFile(fieldSummaryList.build()));
    }

    private static ManifestEvaluator getManifestEvaluator(TupleDomain<ColumnHandle> predicate, org.apache.iceberg.PartitionSpec icebergPartitionSpec)
    {
        TupleDomain<HiveColumnHandle> hivePredicate = predicate.transformKeys(HiveColumnHandle.class::cast);
        return ManifestEvaluator.forRowFilter(toIcebergExpression(hivePredicate), icebergPartitionSpec, false);
    }

    @Nullable
    private static ByteBuffer toIcebergValue(NullableValue partitionValue)
    {
        if (partitionValue.isNull()) {
            return null;
        }
        Type type = partitionValue.getType();
        // hour() transform
        if (type instanceof TimestampType || type instanceof TimeType) {
            long epochMicros = MICROSECONDS.toHours((Long) partitionValue.getValue());
            return Conversions.toByteBuffer(Types.TimestampType.withoutZone(), epochMicros);
        }

        // Trino and Iceberg both store date as days from epoch
        if (type instanceof DateType) {
            final int epochDays = toIntExact(((Long) partitionValue.getValue()));
            return Conversions.toByteBuffer(Types.DateType.get(), epochDays);
        }

        if (type instanceof IntegerType || type instanceof BigintType) {
            final long val = (Long) partitionValue.getValue();
            return Conversions.toByteBuffer(Types.LongType.get(), val);
        }
        throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported type for hidden partitioning: " + type);
    }

    private static Schema toIcebergSchema(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                org.apache.iceberg.types.Type type = toIcebergType(column.getType());
                Types.NestedField field = column.isNullable()
                        ? Types.NestedField.optional(index, column.getName(), type, column.getComment())
                        : Types.NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        Schema schema = new Schema(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        return TypeUtil.assignFreshIds(schema, nextFieldId::getAndIncrement);
    }
}
