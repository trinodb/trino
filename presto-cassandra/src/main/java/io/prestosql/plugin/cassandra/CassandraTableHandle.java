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
package io.prestosql.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CassandraTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<List<CassandraPartition>> partitions;
    private final String clusteringKeyPredicates;

    @JsonCreator
    public CassandraTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this(schemaName, tableName, Optional.empty(), "");
    }

    public CassandraTableHandle(
            String schemaName,
            String tableName,
            Optional<List<CassandraPartition>> partitions,
            String clusteringKeyPredicates)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.clusteringKeyPredicates = requireNonNull(clusteringKeyPredicates, "clusteringKeyPredicates is null");
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

    // do not serialize partitions as they are not needed on workers
    @JsonIgnore
    public Optional<List<CassandraPartition>> getPartitions()
    {
        return partitions;
    }

    // do not serialize clustered predicate as they are not needed on workers
    @JsonIgnore
    public String getClusteringKeyPredicates()
    {
        return clusteringKeyPredicates;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, partitions, clusteringKeyPredicates);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CassandraTableHandle other = (CassandraTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.partitions, other.partitions) &&
                Objects.equals(this.clusteringKeyPredicates, other.clusteringKeyPredicates);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }
}
