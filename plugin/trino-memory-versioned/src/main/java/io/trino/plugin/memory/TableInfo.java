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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.trino.plugin.memory.MemoryTableProperties.KEY_COLUMN;
import static java.util.Objects.requireNonNull;

public class TableInfo
{
    private final long id;
    private final String schemaName;
    private final String tableName;
    private final List<ColumnInfo> columns;
    private final Optional<Integer> keyColumnIndex;
    private final HostAddress hostAddress;

    private final AtomicLong nextVersion = new AtomicLong();
    private final Set<Long> committedVersions = new HashSet<>();

    public TableInfo(long id, String schemaName, String tableName, List<ColumnInfo> columns, Optional<Integer> keyColumnIndex, HostAddress hostAddress)
    {
        this.id = id;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(columns);
        this.keyColumnIndex = requireNonNull(keyColumnIndex, "keyColumnIndex is null");
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
    }

    public long getId()
    {
        return id;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public ConnectorTableMetadata getMetadata()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        keyColumnIndex.map(columns::get).map(ColumnInfo::getName)
                .ifPresent(name -> properties.put(KEY_COLUMN, name));
        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                columns.stream()
                        .map(ColumnInfo::getMetadata)
                        .collect(Collectors.toList()),
                properties.buildOrThrow());
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public ColumnInfo getColumn(MemoryColumnHandle handle)
    {
        return columns.get(handle.getColumnIndex());
    }

    public Optional<Integer> getKeyColumnIndex()
    {
        return keyColumnIndex;
    }

    public HostAddress getHostAddress()
    {
        return hostAddress;
    }

    public long getNextVersion()
    {
        return nextVersion.getAndIncrement();
    }

    public void commitVersion(long version)
    {
        committedVersions.add(version);
    }

    public Set<Long> getCommittedVersions()
    {
        return ImmutableSet.copyOf(committedVersions);
    }
}
