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
package io.trino.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class CassandraTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<List<CassandraPartition>> partitions;
    private final String clusteringKeyPredicates;

    public CassandraTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, Optional.empty(), "");
    }

    @JsonCreator
    public CassandraTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitions") Optional<List<CassandraPartition>> partitions,
            @JsonProperty("clusteringKeyPredicates") String clusteringKeyPredicates)
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

    @JsonProperty
    public Optional<List<CassandraPartition>> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
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
        String result = format("%s:%s", schemaName, tableName);
        if (this.partitions.isPresent()) {
            List<CassandraPartition> partitions = this.partitions.get();
            result += format(
                    " %d partitions %s",
                    partitions.size(),
                    Stream.concat(
                                    partitions.subList(0, Math.min(partitions.size(), 3)).stream(),
                                    partitions.size() > 3 ? Stream.of("...") : Stream.of())
                            .map(Object::toString)
                            .collect(joining(", ", "[", "]")));
        }
        if (!clusteringKeyPredicates.isEmpty()) {
            result += format(" constraint(%s)", clusteringKeyPredicates);
        }
        return result;
    }
}
