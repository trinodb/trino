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
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static java.util.Objects.requireNonNull;

public class TableMetadata
{
    private final Optional<String> writerVersion;
    private final Optional<String> owner;
    private final String tableType;
    private final List<Column> dataColumns;
    private final List<Column> partitionColumns;
    private final Map<String, String> parameters;

    private final Optional<HiveStorageFormat> storageFormat;
    private final Optional<StorageFormat> originalStorageFormat;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final Map<String, String> serdeParameters;

    private final Optional<String> externalLocation;

    private final Optional<String> viewOriginalText;
    private final Optional<String> viewExpandedText;

    private final Map<String, HiveColumnStatistics> columnStatistics;

    @JsonCreator
    public TableMetadata(
            @JsonProperty("writerVersion") Optional<String> writerVersion,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("tableType") String tableType,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("partitionColumns") List<Column> partitionColumns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("storageFormat") Optional<HiveStorageFormat> storageFormat,
            @JsonProperty("originalStorageFormat") Optional<StorageFormat> originalStorageFormat,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("externalLocation") Optional<String> externalLocation,
            @JsonProperty("viewOriginalText") Optional<String> viewOriginalText,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText,
            @JsonProperty("columnStatistics") Map<String, HiveColumnStatistics> columnStatistics)
    {
        this.writerVersion = requireNonNull(writerVersion, "writerVersion is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));

        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.originalStorageFormat = requireNonNull(originalStorageFormat, "originalStorageFormat is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.serdeParameters = requireNonNull(serdeParameters, "serdeParameters is null");
        this.externalLocation = requireNonNull(externalLocation, "externalLocation is null");
        if (tableType.equals(EXTERNAL_TABLE.name())) {
            checkArgument(externalLocation.isPresent(), "External location is required for external tables");
        }
        else {
            checkArgument(externalLocation.isEmpty(), "External location is only allowed for external tables");
        }

        this.viewOriginalText = requireNonNull(viewOriginalText, "viewOriginalText is null");
        this.viewExpandedText = requireNonNull(viewExpandedText, "viewExpandedText is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        checkArgument(partitionColumns.isEmpty() || columnStatistics.isEmpty(), "column statistics cannot be set for partitioned table");
    }

    public TableMetadata(String currentVersion, Table table)
    {
        writerVersion = Optional.of(requireNonNull(currentVersion, "currentVersion is null"));
        owner = table.getOwner();
        tableType = table.getTableType();
        dataColumns = table.getDataColumns();
        partitionColumns = table.getPartitionColumns();
        parameters = table.getParameters();

        StorageFormat tableFormat = table.getStorage().getStorageFormat();
        storageFormat = Arrays.stream(HiveStorageFormat.values())
                .filter(format -> tableFormat.equals(StorageFormat.fromHiveStorageFormat(format)))
                .findFirst();
        if (storageFormat.isPresent()) {
            originalStorageFormat = Optional.empty();
        }
        else {
            originalStorageFormat = Optional.of(tableFormat);
        }
        bucketProperty = table.getStorage().getBucketProperty();
        serdeParameters = table.getStorage().getSerdeParameters();

        if (tableType.equals(EXTERNAL_TABLE.name())) {
            externalLocation = Optional.of(table.getStorage().getLocation());
        }
        else {
            externalLocation = Optional.empty();
        }

        viewOriginalText = table.getViewOriginalText();
        viewExpandedText = table.getViewExpandedText();
        columnStatistics = ImmutableMap.of();
    }

    @JsonProperty
    public Optional<String> getWriterVersion()
    {
        return writerVersion;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonProperty
    public String getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public List<Column> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<Column> getPartitionColumns()
    {
        return partitionColumns;
    }

    public Optional<Column> getColumn(String name)
    {
        for (Column partitionColumn : partitionColumns) {
            if (partitionColumn.getName().equals(name)) {
                return Optional.of(partitionColumn);
            }
        }
        for (Column dataColumn : dataColumns) {
            if (dataColumn.getName().equals(name)) {
                return Optional.of(dataColumn);
            }
        }
        return Optional.empty();
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
    public Optional<StorageFormat> getOriginalStorageFormat()
    {
        return originalStorageFormat;
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
    public Optional<String> getViewOriginalText()
    {
        return viewOriginalText;
    }

    @JsonProperty
    public Optional<String> getViewExpandedText()
    {
        return viewExpandedText;
    }

    @JsonProperty
    public Map<String, HiveColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public TableMetadata withDataColumns(String currentVersion, List<Column> dataColumns)
    {
        return new TableMetadata(
                Optional.of(requireNonNull(currentVersion, "currentVersion is null")),
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                originalStorageFormat,
                bucketProperty,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public TableMetadata withParameters(String currentVersion, Map<String, String> parameters)
    {
        return new TableMetadata(
                Optional.of(requireNonNull(currentVersion, "currentVersion is null")),
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                originalStorageFormat,
                bucketProperty,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public TableMetadata withColumnStatistics(String currentVersion, Map<String, HiveColumnStatistics> columnStatistics)
    {
        return new TableMetadata(
                Optional.of(requireNonNull(currentVersion, "currentVersion is null")),
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                parameters,
                storageFormat,
                originalStorageFormat,
                bucketProperty,
                serdeParameters,
                externalLocation,
                viewOriginalText,
                viewExpandedText,
                columnStatistics);
    }

    public Table toTable(String databaseName, String tableName, String location)
    {
        return new Table(
                databaseName,
                tableName,
                owner,
                tableType,
                Storage.builder()
                        .setLocation(externalLocation.or(() -> Optional.ofNullable(parameters.get(LOCATION_PROPERTY))).orElse(location))
                        .setStorageFormat(storageFormat.map(StorageFormat::fromHiveStorageFormat)
                                .or(() -> originalStorageFormat)
                                .orElse(VIEW_STORAGE_FORMAT))
                        .setBucketProperty(bucketProperty)
                        .setSerdeParameters(serdeParameters)
                        .build(),
                dataColumns,
                partitionColumns,
                parameters,
                viewOriginalText,
                viewExpandedText,
                OptionalLong.empty());
    }
}
