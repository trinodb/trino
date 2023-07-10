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
package io.trino.plugin.raptor.legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.raptor.legacy.util.MetadataUtil.checkSchemaName;
import static io.trino.plugin.raptor.legacy.util.MetadataUtil.checkTableName;
import static java.util.Objects.requireNonNull;

public final class RaptorTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final long tableId;
    private final Optional<Long> distributionId;
    private final Optional<String> distributionName;
    private final OptionalInt bucketCount;
    private final boolean organized;
    private final TupleDomain<RaptorColumnHandle> constraint;
    private final Optional<List<String>> bucketAssignments;

    @JsonCreator
    public RaptorTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("distributionId") Optional<Long> distributionId,
            @JsonProperty("distributionName") Optional<String> distributionName,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("organized") boolean organized,
            @JsonProperty("constraint") TupleDomain<RaptorColumnHandle> constraint,
            // this field will not be in the JSON, but keep it here to avoid duplicating the constructor
            @JsonProperty("bucketAssignments") Optional<List<String>> bucketAssignments)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);

        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;

        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.distributionId = requireNonNull(distributionId, "distributionId is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.organized = organized;

        this.constraint = requireNonNull(constraint, "constraint is null");
        this.bucketAssignments = bucketAssignments.map(ImmutableList::copyOf);
    }

    public boolean isBucketed()
    {
        return this.distributionId.isPresent();
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
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public Optional<Long> getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public Optional<String> getDistributionName()
    {
        return distributionName;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public boolean isOrganized()
    {
        return organized;
    }

    @JsonProperty
    public TupleDomain<RaptorColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonIgnore
    public Optional<List<String>> getBucketAssignments()
    {
        return bucketAssignments;
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName + ":" + tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableId, distributionId, distributionName, bucketCount, organized, constraint, bucketAssignments);
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
        RaptorTableHandle other = (RaptorTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tableId, other.tableId) &&
                Objects.equals(this.distributionId, other.distributionId) &&
                Objects.equals(this.distributionName, other.distributionName) &&
                Objects.equals(this.bucketCount, other.bucketCount) &&
                this.organized == other.organized &&
                Objects.equals(this.constraint, other.constraint) &&
                Objects.equals(this.bucketAssignments, other.bucketAssignments);
    }

    @JsonIgnore
    public Optional<RaptorPartitioningHandle> getPartitioningHandle()
    {
        return distributionId.map(id -> new RaptorPartitioningHandle(id, bucketAssignments.get()));
    }
}
