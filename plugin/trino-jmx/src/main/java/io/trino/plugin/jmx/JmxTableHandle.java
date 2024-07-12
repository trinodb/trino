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
package io.trino.plugin.jmx;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public record JmxTableHandle(
        SchemaTableName tableName,
        List<String> objectNames,
        List<JmxColumnHandle> columnHandles,
        boolean liveData,
        TupleDomain<ColumnHandle> nodeFilter)
        implements ConnectorTableHandle
{
    public JmxTableHandle
    {
        requireNonNull(tableName, "tableName is null");
        objectNames = ImmutableList.copyOf(requireNonNull(objectNames, "objectNames is null"));
        columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        requireNonNull(nodeFilter, "nodeFilter is null");

        checkArgument(!objectNames.isEmpty(), "objectsNames is empty");
    }

    @JsonIgnore
    public ConnectorTableMetadata getTableMetadata()
    {
        return new ConnectorTableMetadata(
                tableName,
                ImmutableList.copyOf(transform(columnHandles, JmxColumnHandle::getColumnMetadata)));
    }
}
