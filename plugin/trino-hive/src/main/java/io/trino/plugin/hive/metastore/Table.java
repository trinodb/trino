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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;

@Immutable
public class Table
{
    private final String databaseName;
    private final String tableName;
    private final Optional<String> owner;
    private final String tableType; // This is not an enum because some Hive implementations define additional table types.
    private final List<Column> dataColumns;
    private final List<Column> partitionColumns;
    private final Storage storage;
    private final Map<String, String> parameters;
    private final Optional<String> viewOriginalText;
    private final Optional<String> viewExpandedText;
    private final OptionalLong writeId;

    @JsonCreator
    public Table(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("tableType") String tableType,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("partitionColumns") List<Column> partitionColumns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("viewOriginalText") Optional<String> viewOriginalText,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText,
            @JsonProperty("writeId") OptionalLong writeId)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.viewOriginalText = requireNonNull(viewOriginalText, "viewOriginalText is null");
        this.viewExpandedText = requireNonNull(viewExpandedText, "viewExpandedText is null");
        this.writeId = requireNonNull(writeId, "writeId is null");
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(databaseName, tableName);
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
        return Stream.concat(partitionColumns.stream(), dataColumns.stream())
                .filter(column -> column.getName().equals(name))
                .findFirst();
    }

    @JsonProperty
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
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
    public OptionalLong getWriteId()
    {
        return writeId;
    }

    public Table withComment(Optional<String> comment)
    {
        Map<String, String> parameters = new LinkedHashMap<>(this.parameters);
        comment.ifPresentOrElse(
                value -> parameters.put(TABLE_COMMENT, value),
                () -> parameters.remove(TABLE_COMMENT));

        return builder(this)
                .setParameters(parameters)
                .build();
    }

    public Table withOwner(HivePrincipal principal)
    {
        // TODO Add role support https://github.com/trinodb/trino/issues/5706
        if (principal.getType() != USER) {
            throw new TrinoException(NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }

        return builder(this)
                .setOwner(Optional.of(principal.getName()))
                .build();
    }

    public Table withColumnComment(String columnName, Optional<String> columnComment)
    {
        boolean found = false;
        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column dataColumn : dataColumns) {
            if (dataColumn.getName().equals(columnName)) {
                if (found) {
                    throw new TrinoException(HIVE_INVALID_METADATA, "Table %s.%s has multiple columns named %s".formatted(databaseName, tableName, columnName));
                }
                dataColumn = dataColumn.withComment(columnComment);
                found = true;
            }
            newDataColumns.add(dataColumn);
        }

        ImmutableList.Builder<Column> newPartitionColumns = ImmutableList.builder();
        for (Column partitionColumn : partitionColumns) {
            if (partitionColumn.getName().equals(columnName)) {
                if (found) {
                    throw new TrinoException(HIVE_INVALID_METADATA, "Table %s.%s has multiple columns named %s".formatted(databaseName, tableName, columnName));
                }
                partitionColumn = partitionColumn.withComment(columnComment);
                found = true;
            }
            newPartitionColumns.add(partitionColumn);
        }

        if (!found) {
            throw new ColumnNotFoundException(getSchemaTableName(), columnName);
        }

        return builder(this)
                .setDataColumns(newDataColumns.build())
                .setPartitionColumns(newPartitionColumns.build())
                .build();
    }

    public Table withAddColumn(Column newColumn)
    {
        if (dataColumns.stream().map(Column::getName).anyMatch(columnName -> columnName.equals(newColumn.getName())) ||
                partitionColumns.stream().map(Column::getName).anyMatch(columnName -> columnName.equals(newColumn.getName()))) {
            throw new TrinoException(ALREADY_EXISTS, "Table %s.%s already has a column named %s".formatted(databaseName, tableName, newColumn.getName()));
        }
        return builder(this)
                .addDataColumn(newColumn)
                .build();
    }

    public Table withDropColumn(String columnName)
    {
        if (partitionColumns.stream().map(Column::getName).anyMatch(columnName::equals)) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition columns");
        }
        if (storage.getBucketProperty().stream().flatMap(bucket -> bucket.getBucketedBy().stream()).anyMatch(columnName::equals) ||
                storage.getBucketProperty().stream().flatMap(bucket -> bucket.getSortedBy().stream()).map(SortingColumn::getColumnName).anyMatch(columnName::equals)) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop bucket column %s.%s.%s".formatted(databaseName, tableName, columnName));
        }

        boolean found = false;
        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column dataColumn : dataColumns) {
            if (dataColumn.getName().equals(columnName)) {
                found = true;
            }
            else {
                newDataColumns.add(dataColumn);
            }
        }
        if (!found) {
            throw new ColumnNotFoundException(getSchemaTableName(), columnName);
        }
        List<Column> dataColumns = newDataColumns.build();
        if (dataColumns.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop the only non-partition column in a table");
        }
        return builder(this)
                .setDataColumns(dataColumns)
                .build();
    }

    public Table withRenameColumn(String oldColumnName, String newColumnName)
    {
        if (partitionColumns.stream().map(Column::getName).anyMatch(oldColumnName::equals)) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }

        if (dataColumns.stream().map(Column::getName).anyMatch(newColumnName::equals) ||
                partitionColumns.stream().map(Column::getName).anyMatch(newColumnName::equals)) {
            throw new TrinoException(ALREADY_EXISTS, "Table %s.%s already has a column named %s".formatted(databaseName, tableName, newColumnName));
        }

        boolean found = false;
        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column dataColumn : dataColumns) {
            if (dataColumn.getName().equals(oldColumnName)) {
                newDataColumns.add(dataColumn.withName(newColumnName));
                found = true;
            }
            else {
                newDataColumns.add(dataColumn);
            }
        }
        if (!found) {
            throw new ColumnNotFoundException(getSchemaTableName(), oldColumnName);
        }

        Builder builder = builder(this)
                .setDataColumns(newDataColumns.build());

        if (storage.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = storage.getBucketProperty().get();

            ImmutableList.Builder<String> newBucketColumns = ImmutableList.builder();
            for (String bucketColumn : bucketProperty.getBucketedBy()) {
                if (bucketColumn.equals(oldColumnName)) {
                    newBucketColumns.add(newColumnName);
                }
                else {
                    newBucketColumns.add(bucketColumn);
                }
            }

            ImmutableList.Builder<SortingColumn> newSortedColumns = ImmutableList.builder();
            for (SortingColumn sortingColumn : bucketProperty.getSortedBy()) {
                if (sortingColumn.getColumnName().equals(oldColumnName)) {
                    newSortedColumns.add(new SortingColumn(newColumnName, sortingColumn.getOrder()));
                }
                else {
                    newSortedColumns.add(sortingColumn);
                }
            }

            builder.getStorageBuilder().setBucketProperty(new HiveBucketProperty(newBucketColumns.build(), bucketProperty.getBucketCount(), newSortedColumns.build()));
        }
        return builder.build();
    }

    public Table withParameters(Map<String, String> newParameters)
    {
        return builder(this)
                .setParameters(newParameters)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Table table)
    {
        return new Builder(table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", databaseName)
                .add("tableName", tableName)
                .add("owner", owner)
                .add("tableType", tableType)
                .add("dataColumns", dataColumns)
                .add("partitionColumns", partitionColumns)
                .add("storage", storage)
                .add("parameters", parameters)
                .add("viewOriginalText", viewOriginalText)
                .add("viewExpandedText", viewExpandedText)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Table table = (Table) o;
        return Objects.equals(databaseName, table.databaseName) &&
                Objects.equals(tableName, table.tableName) &&
                Objects.equals(owner, table.owner) &&
                Objects.equals(tableType, table.tableType) &&
                Objects.equals(dataColumns, table.dataColumns) &&
                Objects.equals(partitionColumns, table.partitionColumns) &&
                Objects.equals(storage, table.storage) &&
                Objects.equals(parameters, table.parameters) &&
                Objects.equals(viewOriginalText, table.viewOriginalText) &&
                Objects.equals(viewExpandedText, table.viewExpandedText);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                databaseName,
                tableName,
                owner,
                tableType,
                dataColumns,
                partitionColumns,
                storage,
                parameters,
                viewOriginalText,
                viewExpandedText);
    }

    public static class Builder
    {
        private final Storage.Builder storageBuilder;
        private String databaseName;
        private String tableName;
        private Optional<String> owner;
        private String tableType;
        private List<Column> dataColumns = new ArrayList<>();
        private List<Column> partitionColumns = new ArrayList<>();
        private Map<String, String> parameters = new LinkedHashMap<>();
        private Optional<String> viewOriginalText = Optional.empty();
        private Optional<String> viewExpandedText = Optional.empty();
        private OptionalLong writeId = OptionalLong.empty();

        private Builder()
        {
            storageBuilder = Storage.builder();
        }

        private Builder(Table table)
        {
            databaseName = table.databaseName;
            tableName = table.tableName;
            owner = table.owner;
            tableType = table.tableType;
            storageBuilder = Storage.builder(table.getStorage());
            dataColumns = new ArrayList<>(table.dataColumns);
            partitionColumns = new ArrayList<>(table.partitionColumns);
            parameters = new LinkedHashMap<>(table.parameters);
            viewOriginalText = table.viewOriginalText;
            viewExpandedText = table.viewExpandedText;
            writeId = table.writeId;
        }

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setOwner(Optional<String> owner)
        {
            this.owner = owner;
            return this;
        }

        public Builder setTableType(String tableType)
        {
            this.tableType = tableType;
            return this;
        }

        public Storage.Builder getStorageBuilder()
        {
            return storageBuilder;
        }

        public Builder setDataColumns(List<Column> dataColumns)
        {
            this.dataColumns = new ArrayList<>(dataColumns);
            return this;
        }

        public Builder addDataColumn(Column dataColumn)
        {
            this.dataColumns.add(dataColumn);
            return this;
        }

        public Builder setPartitionColumns(List<Column> partitionColumns)
        {
            this.partitionColumns = new ArrayList<>(partitionColumns);
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = new LinkedHashMap<>(parameters);
            return this;
        }

        public Builder setParameter(String key, String value)
        {
            this.parameters.put(key, value);
            return this;
        }

        public Builder setParameter(String key, Optional<String> value)
        {
            if (value.isEmpty()) {
                this.parameters.remove(key);
            }
            else {
                this.parameters.put(key, value.get());
            }
            return this;
        }

        public Builder setViewOriginalText(Optional<String> viewOriginalText)
        {
            this.viewOriginalText = viewOriginalText;
            return this;
        }

        public Builder setViewExpandedText(Optional<String> viewExpandedText)
        {
            this.viewExpandedText = viewExpandedText;
            return this;
        }

        public Builder setWriteId(OptionalLong writeId)
        {
            this.writeId = writeId;
            return this;
        }

        public Builder withStorage(Consumer<Storage.Builder> consumer)
        {
            consumer.accept(storageBuilder);
            return this;
        }

        public Builder apply(Function<Builder, Builder> function)
        {
            return function.apply(this);
        }

        public Table build()
        {
            return new Table(
                    databaseName,
                    tableName,
                    owner,
                    tableType,
                    storageBuilder.build(),
                    dataColumns,
                    partitionColumns,
                    parameters,
                    viewOriginalText,
                    viewExpandedText,
                    writeId);
        }
    }
}
