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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import org.apache.avro.Schema;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.assertj.core.util.Strings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Defines the specification for a Hudi test table, including its schema,
 * partitioning, table type, and the data/commits to create.
 * <p>
 * Subclasses should implement the {@link #executeCommits(HoodieJavaWriteClient)}
 * method to define what data is written to the table and in what order.
 */
public abstract class HudiTableDefinition
{
    /**
     * Hudi metadata columns that are automatically added to all Hudi tables.
     */
    protected static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()));

    private final String tableName;
    private final HoodieTableType tableType;
    private final List<Column> regularColumns;
    private final List<Column> partitionColumns;
    private final String recordKeyField;
    private final String preCombineField;
    private final Map<String, String> partitions;

    protected HudiTableDefinition(
            String tableName,
            HoodieTableType tableType,
            List<Column> regularColumns,
            List<Column> partitionColumns,
            String recordKeyField,
            String preCombineField,
            Map<String, String> partitions)
    {
        this.tableName = tableName;
        this.tableType = tableType;
        this.regularColumns = ImmutableList.copyOf(regularColumns);
        this.partitionColumns = ImmutableList.copyOf(partitionColumns);
        this.recordKeyField = recordKeyField;
        this.preCombineField = preCombineField;
        this.partitions = ImmutableMap.copyOf(partitions);
    }

    /**
     * Constructor for non-partitioned tables.
     */
    protected HudiTableDefinition(
            String tableName,
            HoodieTableType tableType,
            List<Column> regularColumns,
            String recordKeyField,
            String preCombineField)
    {
        this(tableName, tableType, regularColumns, ImmutableList.of(), recordKeyField, preCombineField, ImmutableMap.of());
    }

    /**
     * Returns the name of this table.
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Returns the Hudi table type (COPY_ON_WRITE or MERGE_ON_READ).
     */
    public HoodieTableType getTableType()
    {
        return tableType;
    }

    /**
     * Returns all data columns (Hudi metadata columns + regular columns).
     */
    public List<Column> getDataColumns()
    {
        return Stream.of(HUDI_META_COLUMNS, regularColumns)
                .flatMap(Collection::stream)
                .toList();
    }

    /**
     * Returns the partition columns for this table.
     */
    public List<Column> getPartitionColumns()
    {
        return partitionColumns;
    }

    /**
     * Returns the partitions to register with the metastore.
     * Map key is the partition name (e.g., "dt=2021-12-09/hh=10"),
     * Map value is the partition path (e.g., "dt=2021-12-09/hh=10").
     */
    public Map<String, String> getPartitions()
    {
        return partitions;
    }

    /**
     * Returns the record key field name.
     */
    public String getRecordKeyField()
    {
        return recordKeyField;
    }

    /**
     * Returns the pre-combine field name (required for MERGE_ON_READ tables).
     */
    public Optional<String> getPreCombineField()
    {
        return Optional.ofNullable(preCombineField);
    }

    /**
     * Creates the HoodieWriteConfig for this table with deterministic settings
     * to ensure reproducible test results.
     */
    @SuppressWarnings("deprecation")
    public HoodieWriteConfig createWriteConfig(String basePath)
    {
        TypedProperties properties = new TypedProperties();
        properties.setProperty("hoodie.table.type", tableType.name());

        // Add partition fields if this is a partitioned table
        if (!partitionColumns.isEmpty()) {
            String[] partitionFields = partitionColumns.stream()
                    .map(Column::getName)
                    .toArray(String[]::new);
            properties.put("hoodie.datasource.write.partitionpath.field", Strings.concat(partitionFields, ","));
        }
        HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
                .withPath(basePath)
                .withSchema(getAvroSchema().toString())
                .forTable(tableName)
                .withProps(properties)
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
                .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                        .enable(false)
                        .build())
                .withIndexConfig(HoodieIndexConfig.newBuilder()
                        .withIndexType(HoodieIndex.IndexType.INMEMORY)
                        .build());

        if (preCombineField != null) {
            builder.withPreCombineField(preCombineField);
        }

        return builder.build();
    }

    /**
     * Returns the Avro schema for this table.
     * <p>
     * The schema defines the structure of the data records and is required
     * by Hudi for writing data.
     *
     * @return the Avro schema
     */
    protected abstract Schema getAvroSchema();

    /**
     * Executes the commits that populate this table with data.
     * <p>
     * Subclasses must implement this method to define what data is written
     * and in what order (e.g., initial insert followed by updates).
     * <p>
     * Use deterministic commit timestamps (via {@link HoodieJavaWriteClient})
     * to ensure reproducible test results.
     *
     * @param client the Hudi write client to use for writing data
     */
    public abstract void executeCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws Exception;

    /**
     * Helper method to create a Column with the specified name and type.
     */
    protected static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Map.of());
    }
}
