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
package io.trino.connector.system;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record SystemColumnHandle(String columnName)
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(SystemColumnHandle.class);

    public SystemColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(columnName);
    }

    public static Map<String, ColumnHandle> toSystemColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream().collect(toImmutableMap(
                ColumnMetadata::getName,
                column -> new SystemColumnHandle(column.getName())));
    }
}
