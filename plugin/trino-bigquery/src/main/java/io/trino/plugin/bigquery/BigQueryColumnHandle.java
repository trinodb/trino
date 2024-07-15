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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record BigQueryColumnHandle(
        String name,
        Type trinoType,
        StandardSQLTypeName bigqueryType,
        boolean isPushdownSupported,
        Field.Mode mode,
        List<BigQueryColumnHandle> subColumns,
        String description,
        boolean hidden)
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(BigQueryColumnHandle.class);

    public BigQueryColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
        requireNonNull(bigqueryType, "bigqueryType is null");
        requireNonNull(mode, "mode is null");
        subColumns = ImmutableList.copyOf(requireNonNull(subColumns, "subColumns is null"));
    }

    @JsonIgnore
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

    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(name)
                + estimatedSizeOf(subColumns, BigQueryColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(description);
    }
}
