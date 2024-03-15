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
package io.trino.plugin.deltalake.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.Snapshot;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class KernelDeltaLakeTableHandle
        implements LocatedTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final boolean managed;
    private final String location;
    private final Optional<Set<DeltaLakeColumnHandle>> projectedColumns;
    private final long readVersion;

    // Snapshot of the table being read. Initialized when table scanshot is created.
    // Used to avoid repeating the scan creation. This is not serialized and sent to workers.
    private Optional<Snapshot> deltaSnapshot = Optional.empty();

    @JsonCreator
    public KernelDeltaLakeTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("managed") boolean managed,
            @JsonProperty("location") String location,
            @JsonProperty("projectedColumns") Optional<Set<DeltaLakeColumnHandle>> projectedColumns,
            @JsonProperty("readVersion") long readVersion)
    {
        this(schemaName, tableName, managed, location, projectedColumns, readVersion, Optional.empty() /* deltaScan */);
    }

    public KernelDeltaLakeTableHandle(
            String schemaName,
            String tableName,
            boolean managed,
            String location,
            Optional<Set<DeltaLakeColumnHandle>> projectedColumns,
            long readVersion,
            Optional<Snapshot> deltaSnapshot)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.managed = managed;
        this.location = location;
        this.projectedColumns = projectedColumns;
        this.readVersion = readVersion;
        this.deltaSnapshot = deltaSnapshot;
    }

    public KernelDeltaLakeTableHandle withSnapshot(Snapshot snapshot)
    {
        return new KernelDeltaLakeTableHandle(schemaName, tableName, managed, location, projectedColumns, readVersion, Optional.of(snapshot));
    }

    @Override
    public SchemaTableName schemaTableName()
    {
        return getSchemaTableName();
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public boolean managed()
    {
        return isManaged();
    }

    @JsonProperty
    public boolean isManaged()
    {
        return managed;
    }

    @Override
    public String location()
    {
        return getLocation();
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Optional<Set<DeltaLakeColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public long getReadVersion()
    {
        return readVersion;
    }

    @JsonIgnore
    public Optional<Snapshot> getDeltaSnapshot()
    {
        return deltaSnapshot;
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
        KernelDeltaLakeTableHandle that = (KernelDeltaLakeTableHandle) o;
        return managed == that.managed && readVersion == that.readVersion &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(location, that.location);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, managed, location, readVersion);
    }
}
