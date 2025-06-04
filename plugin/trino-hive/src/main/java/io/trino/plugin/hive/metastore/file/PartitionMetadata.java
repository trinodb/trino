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
package io.trino.plugin.hive.metastore.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveStorageFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.metastore.file.ColumnStatistics.fromHiveColumnStatistics;
import static java.util.Objects.requireNonNull;

public class PartitionMetadata
{
    private final List<Column> columns;
    private final Map<String, String> parameters;

    private final Optional<HiveStorageFormat> storageFormat;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final Map<String, String> serdeParameters;

    private final Optional<String> externalLocation;

    private final Map<String, ColumnStatistics> columnStatistics;

    @JsonCreator
    public PartitionMetadata(
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("storageFormat") Optional<HiveStorageFormat> storageFormat,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("externalLocation") Optional<String> externalLocation,
            @JsonProperty("columnStatistics") Map<String, ColumnStatistics> columnStatistics)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));

        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.serdeParameters = requireNonNull(serdeParameters, "serdeParameters is null");

        this.externalLocation = requireNonNull(externalLocation, "externalLocation is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    public PartitionMetadata(Table table, PartitionWithStatistics partitionWithStatistics)
    {
        Partition partition = partitionWithStatistics.getPartition();
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();

        this.columns = partition.getColumns();
        this.parameters = updateStatisticsParameters(partition.getParameters(), statistics.basicStatistics());

        StorageFormat tableFormat = partition.getStorage().getStorageFormat();
        storageFormat = Arrays.stream(HiveStorageFormat.values())
                .filter(format -> tableFormat.equals(format.toStorageFormat()))
                .findFirst();

        if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            externalLocation = Optional.of(partition.getStorage().getLocation());
        }
        else {
            externalLocation = Optional.empty();
        }

        bucketProperty = partition.getStorage().getBucketProperty();
        serdeParameters = partition.getStorage().getSerdeParameters();
        columnStatistics = statistics.columnStatistics().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> fromHiveColumnStatistics(entry.getValue())));
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @JsonProperty
    public Optional<HiveStorageFormat> getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    public Optional<HiveBucketProperty> getBucketProperty()
    {
        return bucketProperty;
    }

    @JsonProperty
    public Map<String, String> getSerdeParameters()
    {
        return serdeParameters;
    }

    @JsonProperty
    public Optional<String> getExternalLocation()
    {
        return externalLocation;
    }

    @JsonProperty
    public Map<String, ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public PartitionMetadata withParameters(Map<String, String> parameters)
    {
        return new PartitionMetadata(columns, parameters, storageFormat, bucketProperty, serdeParameters, externalLocation, columnStatistics);
    }

    public PartitionMetadata withColumnStatistics(Map<String, ColumnStatistics> columnStatistics)
    {
        return new PartitionMetadata(columns, parameters, storageFormat, bucketProperty, serdeParameters, externalLocation, columnStatistics);
    }

    public Partition toPartition(String databaseName, String tableName, List<String> values, String location)
    {
        return new Partition(
                databaseName,
                tableName,
                values,
                Storage.builder()
                        .setLocation(externalLocation.orElse(location))
                        .setStorageFormat(storageFormat.map(HiveStorageFormat::toStorageFormat).orElse(VIEW_STORAGE_FORMAT))
                        .setBucketProperty(bucketProperty)
                        .setSerdeParameters(serdeParameters)
                        .build(),
                columns,
                parameters);
    }
}
