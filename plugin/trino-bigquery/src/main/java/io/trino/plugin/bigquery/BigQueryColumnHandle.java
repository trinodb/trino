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
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BigQueryColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(BigQueryColumnHandle.class);

    private final String name;
    private final Type trinoType;
    private final StandardSQLTypeName bigqueryType;
    private final Field.Mode mode;
    private final Long precision;
    private final Long scale;
    private final List<BigQueryColumnHandle> subColumns;
    private final String description;
    private final boolean hidden;

    @JsonCreator
    public BigQueryColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("trinoType") Type trinoType,
            @JsonProperty("bigqueryType") StandardSQLTypeName bigqueryType,
            @JsonProperty("mode") Field.Mode mode,
            @JsonProperty("precision") Long precision,
            @JsonProperty("scale") Long scale,
            @JsonProperty("subColumns") List<BigQueryColumnHandle> subColumns,
            @JsonProperty("description") String description,
            @JsonProperty("hidden") boolean hidden)
    {
        this.name = requireNonNull(name, "column name cannot be null");
        this.trinoType = requireNonNull(trinoType, "trinoType is null");
        this.bigqueryType = requireNonNull(bigqueryType, "bigqueryType is null");
        this.mode = requireNonNull(mode, "Field mode cannot be null");
        this.precision = precision;
        this.scale = scale;
        this.subColumns = ImmutableList.copyOf(requireNonNull(subColumns, "subColumns is null"));
        this.description = description;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getTrinoType()
    {
        return trinoType;
    }

    @JsonProperty
    public StandardSQLTypeName getBigqueryType()
    {
        return bigqueryType;
    }

    @JsonProperty
    public Field.Mode getMode()
    {
        return mode;
    }

    @JsonProperty
    public Long getPrecision()
    {
        return precision;
    }

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
                .setType(trinoType)
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
                Objects.equals(trinoType, that.trinoType) &&
                Objects.equals(bigqueryType, that.bigqueryType) &&
                Objects.equals(mode, that.mode) &&
                Objects.equals(precision, that.precision) &&
                Objects.equals(scale, that.scale) &&
                Objects.equals(subColumns, that.subColumns) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, trinoType, bigqueryType, mode, precision, scale, subColumns, description);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("trinoType", trinoType)
                .add("bigqueryType", bigqueryType)
                .add("mode", mode)
                .add("precision", precision)
                .add("scale", scale)
                .add("subColumns", subColumns)
                .add("description", description)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(name)
                + sizeOf(precision)
                + sizeOf(scale)
                + estimatedSizeOf(subColumns, BigQueryColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(description);
    }
}
