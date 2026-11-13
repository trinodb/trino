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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.FieldSchema;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveType;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA;

/**
 * Maps a Lakehouse storage descriptor and its columns to the Trino metastore model.
 *
 * <p>Tables and partitions carry the same descriptor, so both go through this class.
 */
public final class StorageMapper
{
    private StorageMapper() {}

    public static Storage toTrino(StorageDescriptor descriptor, String qualifiedName)
    {
        if (!descriptor.hasSerdeInfo()) {
            throw new TrinoException(
                    HIVE_INVALID_METADATA, "Storage descriptor has no SerDe info: " + qualifiedName);
        }
        SerdeInfo serdeInfo = descriptor.getSerdeInfo();

        return new Storage(
                StorageFormat.createNullable(
                        emptyToNull(serdeInfo.getSerializationLib()),
                        emptyToNull(descriptor.getInputFormat()),
                        emptyToNull(descriptor.getOutputFormat())),
                optionalText(descriptor.getLocationUri()),
                bucketProperty(descriptor, qualifiedName),
                !descriptor.getSkewedInfo().getSkewedColNamesList().isEmpty(),
                serdeInfo.getParametersMap());
    }

    public static List<Column> toTrinoColumns(List<FieldSchema> fields, String qualifiedName)
    {
        return fields.stream()
                .map(field -> toTrinoColumn(field, qualifiedName))
                .collect(toImmutableList());
    }

    private static Column toTrinoColumn(FieldSchema field, String qualifiedName)
    {
        HiveType type;
        try {
            type = HiveType.valueOf(field.getType().toLowerCase(Locale.ROOT));
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    HIVE_INVALID_METADATA,
                    "Column %s of %s has an invalid type: %s".formatted(field.getName(), qualifiedName, field.getType()),
                    e);
        }
        return new Column(field.getName(), type, optionalText(field.getComment()), ImmutableMap.of());
    }

    private static Optional<HiveBucketProperty> bucketProperty(StorageDescriptor descriptor, String qualifiedName)
    {
        if (descriptor.getNumBuckets() <= 0) {
            return Optional.empty();
        }
        if (descriptor.getBucketColsList().isEmpty()) {
            throw new TrinoException(
                    HIVE_INVALID_METADATA,
                    "Storage descriptor sets a bucket count without bucket columns: " + qualifiedName);
        }

        List<SortingColumn> sortedBy = descriptor.getSortColsList().stream()
                .map(order -> new SortingColumn(order.getCol(), sortOrder(order.getOrder(), qualifiedName)))
                .collect(toImmutableList());

        return Optional.of(new HiveBucketProperty(
                descriptor.getBucketColsList(),
                descriptor.getNumBuckets(),
                sortedBy));
    }

    private static SortingColumn.Order sortOrder(int order, String qualifiedName)
    {
        return switch (order) {
            case 0 -> SortingColumn.Order.DESCENDING;
            case 1 -> SortingColumn.Order.ASCENDING;
            default -> throw new TrinoException(
                    HIVE_INVALID_METADATA, "Storage descriptor has an invalid sorting order: " + qualifiedName);
        };
    }

    static Optional<String> optionalText(String value)
    {
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }

    private static String emptyToNull(String value)
    {
        return value.isEmpty() ? null : value;
    }
}
