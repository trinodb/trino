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
package io.prestosql.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.Field;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class BigQueryColumnHandle
        implements ColumnHandle, BigQueryType.Adaptor
{
    private final String name;
    private final BigQueryType bigQueryType;
    private final Field.Mode mode;
    private final List<BigQueryColumnHandle> subColumns;
    private final String description;

    @JsonCreator
    public BigQueryColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("bigQueryType") BigQueryType bigQueryType,
            @JsonProperty("mode") Field.Mode mode,
            @JsonProperty("subColumns") List<BigQueryColumnHandle> subColumns,
            @JsonProperty("description") String description)
    {
        this.name = requireNonNull(name, "column name cannot be null");
        this.bigQueryType = requireNonNull(bigQueryType, "column type cannot be null for column [" + name + "]");
        this.mode = mode != null ? mode : Field.Mode.NULLABLE;
        this.subColumns = subColumns != null ? subColumns : emptyList();
        this.description = description;
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
    public ImmutableMap<String, BigQueryType.Adaptor> getBigQuerySubTypes()
    {
        return subColumns.stream().collect(ImmutableMap.toImmutableMap(c -> c.name, c -> c));
    }

    @Override
    @JsonProperty
    public Field.Mode getMode()
    {
        return mode;
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

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(getPrestoType())
                .setComment(Optional.ofNullable(description))
                .setNullable(mode == Field.Mode.NULLABLE)
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
        return name.equals(that.name) &&
                bigQueryType == that.bigQueryType &&
                mode == that.mode &&
                subColumns.equals(that.subColumns) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, bigQueryType, mode, subColumns, description);
    }

    @Override
    public String toString()
    {
        return "BigQueryColumnHandle{" +
                "name='" + name + '\'' +
                ", type=" + bigQueryType +
                ", mode=" + mode +
                ", subColumns=" + subColumns +
                ", description='" + description + '\'' +
                '}';
    }
}
