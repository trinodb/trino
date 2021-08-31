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
package io.trino.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.Field;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryColumnHandle
        implements ColumnHandle, BigQueryType.Adaptor
{
    private final String name;
    private final BigQueryType bigQueryType;
    private final Field.Mode mode;
    private final Long precision;
    private final Long scale;
    private final List<BigQueryColumnHandle> subColumns;
    private final String description;
    private final boolean hidden;

    @JsonCreator
    public BigQueryColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("bigQueryType") BigQueryType bigQueryType,
            @JsonProperty("mode") Field.Mode mode,
            @JsonProperty("precision") Long precision,
            @JsonProperty("scale") Long scale,
            @JsonProperty("subColumns") List<BigQueryColumnHandle> subColumns,
            @JsonProperty("description") String description,
            @JsonProperty("hidden") boolean hidden)
    {
        this.name = requireNonNull(name, "column name cannot be null");
        this.bigQueryType = requireNonNull(bigQueryType, () -> format("column type cannot be null for column [%s]", name));
        this.mode = requireNonNull(mode, "Field mode cannot be null");
        this.precision = precision;
        this.scale = scale;
        this.subColumns = ImmutableList.copyOf(requireNonNull(subColumns, "subColumns is null"));
        this.description = description;
        this.hidden = hidden;
    }

    @VisibleForTesting
    BigQueryColumnHandle(
            String name,
            BigQueryType bigQueryType,
            Field.Mode mode,
            Long precision,
            Long scale,
            List<BigQueryColumnHandle> subColumns,
            String description)
    {
        this(name, bigQueryType, mode, precision, scale, subColumns, description, false);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public BigQueryType getBigQueryType()
    {
        return bigQueryType;
    }

    @Override
    public Map<String, BigQueryType.Adaptor> getBigQuerySubTypes()
    {
        return subColumns.stream().collect(toImmutableMap(BigQueryColumnHandle::getName, column -> column));
    }

    @Override
    @JsonProperty
    public Field.Mode getMode()
    {
        return mode;
    }

    @Override
    @JsonProperty
    public Long getPrecision()
    {
        return precision;
    }

    @Override
    @JsonProperty
    public Long getScale()
    {
        return scale;
    }

    @JsonProperty
    public List<BigQueryColumnHandle> getSubColumns()
    {
        return subColumns;
    }

    @JsonProperty
    public String description()
    {
        return description;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(getTrinoType())
                .setComment(Optional.ofNullable(description))
                .setNullable(mode == Field.Mode.NULLABLE)
                .setHidden(hidden)
                .build();
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
        BigQueryColumnHandle that = (BigQueryColumnHandle) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(bigQueryType, that.bigQueryType) &&
                Objects.equals(mode, that.mode) &&
                Objects.equals(precision, that.precision) &&
                Objects.equals(scale, that.scale) &&
                Objects.equals(subColumns, that.subColumns) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, bigQueryType, mode, precision, scale, subColumns, description);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", bigQueryType)
                .add("mode", mode)
                .add("precision", precision)
                .add("scale", scale)
                .add("subColumns", subColumns)
                .add("description", description)
                .toString();
    }
}
