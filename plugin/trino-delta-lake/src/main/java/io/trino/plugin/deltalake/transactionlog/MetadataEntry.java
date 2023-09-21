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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.COLUMN_MAPPING_MODE_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.MAX_COLUMN_ID_CONFIGURATION_KEY;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class MetadataEntry
{
    public static final String DELTA_CHECKPOINT_WRITE_STATS_AS_JSON_PROPERTY = "delta.checkpoint.writeStatsAsJson";
    public static final String DELTA_CHECKPOINT_WRITE_STATS_AS_STRUCT_PROPERTY = "delta.checkpoint.writeStatsAsStruct";
    public static final String DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY = "delta.enableChangeDataFeed";

    private static final String DELTA_CHECKPOINT_INTERVAL_PROPERTY = "delta.checkpointInterval";

    private final String id;
    private final String name;
    private final String description;
    private final Format format;
    private final String schemaString;
    private final List<String> partitionColumns;
    private final List<String> canonicalPartitionColumns;
    private final Map<String, String> configuration;
    private final long createdTime;

    @JsonCreator
    public MetadataEntry(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("format") Format format,
            @JsonProperty("schemaString") String schemaString,
            @JsonProperty("partitionColumns") List<String> partitionColumns,
            @JsonProperty("configuration") Map<String, String> configuration,
            @JsonProperty("createdTime") long createdTime)
    {
        this.id = id;
        this.name = name;
        this.description = description;
        this.format = format;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
        this.canonicalPartitionColumns = partitionColumns.stream()
                // canonicalize partition keys to lowercase so they match column names used in DeltaLakeColumnHandle
                .map(value -> value.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        this.configuration = configuration;
        this.createdTime = createdTime;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public Format getFormat()
    {
        return format;
    }

    @JsonProperty
    public String getSchemaString()
    {
        return schemaString;
    }

    /**
     * For use in write-path. Returns partition column names with case preserved.
     */
    @JsonProperty("partitionColumns")
    public List<String> getOriginalPartitionColumns()
    {
        return partitionColumns;
    }

    /**
     * For use in read-path. Returns lowercase partition column names.
     */
    @JsonIgnore
    public List<String> getLowercasePartitionColumns()
    {
        return canonicalPartitionColumns;
    }

    @JsonProperty
    public Map<String, String> getConfiguration()
    {
        return configuration;
    }

    @JsonProperty
    public long getCreatedTime()
    {
        return createdTime;
    }

    @JsonIgnore
    public Optional<Long> getCheckpointInterval()
    {
        if (this.getConfiguration() == null) {
            return Optional.empty();
        }

        String value = this.getConfiguration().get(DELTA_CHECKPOINT_INTERVAL_PROPERTY);
        if (value == null) {
            return Optional.empty();
        }

        try {
            long tableCheckpointInterval = parseLong(value);
            if (tableCheckpointInterval <= 0) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("%s must be greater than 0", DELTA_CHECKPOINT_INTERVAL_PROPERTY));
            }
            return Optional.of(tableCheckpointInterval);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid value for %s property: %s", DELTA_CHECKPOINT_INTERVAL_PROPERTY, value));
        }
    }

    @JsonIgnore
    public Optional<Boolean> isChangeDataFeedEnabled()
    {
        if (this.getConfiguration() == null) {
            return Optional.empty();
        }

        String value = this.getConfiguration().get(DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY);
        if (value == null) {
            return Optional.empty();
        }

        boolean changeDataFeedEnabled = Boolean.parseBoolean(value);
        return Optional.of(changeDataFeedEnabled);
    }

    public static Map<String, String> configurationForNewTable(
            Optional<Long> checkpointInterval,
            Optional<Boolean> changeDataFeedEnabled,
            ColumnMappingMode columnMappingMode,
            OptionalInt maxFieldId)
    {
        ImmutableMap.Builder<String, String> configurationMapBuilder = ImmutableMap.builder();
        checkpointInterval.ifPresent(interval -> configurationMapBuilder.put(DELTA_CHECKPOINT_INTERVAL_PROPERTY, String.valueOf(interval)));
        changeDataFeedEnabled.ifPresent(enabled -> configurationMapBuilder.put(DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY, String.valueOf(enabled)));
        switch (columnMappingMode) {
            case NONE -> { /* do nothing */ }
            case ID, NAME -> {
                configurationMapBuilder.put(COLUMN_MAPPING_MODE_CONFIGURATION_KEY, columnMappingMode.name().toLowerCase(ENGLISH));
                configurationMapBuilder.put(MAX_COLUMN_ID_CONFIGURATION_KEY, String.valueOf(maxFieldId.orElseThrow()));
            }
            case UNKNOWN -> throw new UnsupportedOperationException();
        }
        return configurationMapBuilder.buildOrThrow();
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
        MetadataEntry that = (MetadataEntry) o;
        return createdTime == that.createdTime &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(description, that.description) &&
                Objects.equals(format, that.format) &&
                Objects.equals(schemaString, that.schemaString) &&
                Objects.equals(partitionColumns, that.partitionColumns) &&
                Objects.equals(canonicalPartitionColumns, that.canonicalPartitionColumns) &&
                Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, description, format, schemaString, partitionColumns, canonicalPartitionColumns, configuration, createdTime);
    }

    @Override
    public String toString()
    {
        return format("MetadataEntry{id=%s, name=%s, description=%s, format=%s, schemaString=%s, partitionColumns=%s, configuration=%s, createdTime=%d}",
                id, name, description, format, schemaString, partitionColumns, configuration, createdTime);
    }

    public static class Format
    {
        private final String provider;
        private final Map<String, String> options;

        @JsonCreator
        public Format(
                @JsonProperty("provider") String provider,
                @JsonProperty("options") Map<String, String> options)
        {
            this.provider = provider;
            this.options = options;
        }

        @JsonProperty
        public String getProvider()
        {
            return provider;
        }

        @JsonProperty
        public Map<String, String> getOptions()
        {
            return options;
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
            Format format = (Format) o;
            return Objects.equals(provider, format.provider) &&
                    Objects.equals(options, format.options);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(provider, options);
        }

        @Override
        public String toString()
        {
            return format("MetadataEntry.Format{provider=%s, options=%s}", provider, options);
        }
    }
}
