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
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BigQueryTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final RemoteTableName remoteTableName;
    private final String type;
    private final Optional<BigQueryPartitionType> partitionType;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> projectedColumns;

    @JsonCreator
    public BigQueryTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("type") String type,
            @JsonProperty("partitionType") Optional<BigQueryPartitionType> partitionType,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<List<ColumnHandle>> projectedColumns)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.type = requireNonNull(type, "type is null");
        this.partitionType = requireNonNull(partitionType, "partitionType is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
    }

    public BigQueryTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, TableInfo tableInfo)
    {
        this(
                schemaTableName,
                remoteTableName,
                tableInfo.getDefinition().getType().toString(),
                getPartitionType(tableInfo.getDefinition()),
                TupleDomain.all(),
                Optional.empty());
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<BigQueryPartitionType> getPartitionType()
    {
        return partitionType;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
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
        BigQueryTableHandle that = (BigQueryTableHandle) o;
        // NOTE remoteTableName is not compared here because two handles differing in only remoteTableName will create ambiguity
        // TODO: Add tests for this (see TestJdbcTableHandle#testEquivalence for reference)
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(type, that.type) &&
                Objects.equals(partitionType, that.partitionType) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, type, partitionType, constraint, projectedColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("remoteTableName", remoteTableName)
                .add("schemaTableName", schemaTableName)
                .add("type", type)
                .add("partitionType", partitionType)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .toString();
    }

    BigQueryTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new BigQueryTableHandle(schemaTableName, remoteTableName, type, partitionType, newConstraint, projectedColumns);
    }

    BigQueryTableHandle withProjectedColumns(List<ColumnHandle> newProjectedColumns)
    {
        return new BigQueryTableHandle(schemaTableName, remoteTableName, type, partitionType, constraint, Optional.of(newProjectedColumns));
    }

    public enum BigQueryPartitionType
    {
        TIME,
        INGESTION,
        RANGE,
        /**/
    }

    private static Optional<BigQueryPartitionType> getPartitionType(TableDefinition definition)
    {
        if (definition instanceof StandardTableDefinition) {
            StandardTableDefinition standardTableDefinition = (StandardTableDefinition) definition;
            RangePartitioning rangePartition = standardTableDefinition.getRangePartitioning();
            if (rangePartition != null) {
                return Optional.of(BigQueryPartitionType.RANGE);
            }

            TimePartitioning timePartition = standardTableDefinition.getTimePartitioning();
            if (timePartition != null) {
                if (timePartition.getField() != null) {
                    return Optional.of(BigQueryPartitionType.TIME);
                }
                return Optional.of(BigQueryPartitionType.INGESTION);
            }
        }
        return Optional.empty();
    }
}
