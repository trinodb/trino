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
import com.fasterxml.jackson.annotation.JsonIgnore;
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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BigQueryTableHandle
        implements ConnectorTableHandle
{
    private final BigQueryRelationHandle relationHandle;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<BigQueryColumnHandle>> projectedColumns;

    @JsonCreator
    public BigQueryTableHandle(
            @JsonProperty("relationHandle") BigQueryRelationHandle relationHandle,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<List<BigQueryColumnHandle>> projectedColumns)
    {
        this.relationHandle = requireNonNull(relationHandle, "relationHandle is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
    }

    @Deprecated
    public BigQueryTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, TableInfo tableInfo)
    {
        this(new BigQueryNamedRelationHandle(schemaTableName, remoteTableName, tableInfo.getDefinition().getType().toString(), getPartitionType(tableInfo.getDefinition()), Optional.ofNullable(tableInfo.getDescription())));
    }

    public BigQueryTableHandle(BigQueryRelationHandle relationHandle)
    {
        this(
                relationHandle,
                TupleDomain.all(),
                Optional.empty());
    }

    @JsonProperty
    public BigQueryRelationHandle getRelationHandle()
    {
        return relationHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<BigQueryColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonIgnore
    public BigQueryNamedRelationHandle getRequiredNamedRelation()
    {
        checkState(isNamedRelation(), "The table handle does not represent a named relation: %s", this);
        return (BigQueryNamedRelationHandle) relationHandle;
    }

    @JsonIgnore
    public boolean isSynthetic()
    {
        return !isNamedRelation();
    }

    @JsonIgnore
    public boolean isNamedRelation()
    {
        return relationHandle instanceof BigQueryNamedRelationHandle;
    }

    public BigQueryNamedRelationHandle asPlainTable()
    {
        checkState(!isSynthetic(), "The table handle does not represent a plain table: %s", this);
        return getRequiredNamedRelation();
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
        return Objects.equals(relationHandle, that.relationHandle) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationHandle, constraint, projectedColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relationHandle", relationHandle)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .toString();
    }

    BigQueryTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new BigQueryTableHandle(relationHandle, newConstraint, projectedColumns);
    }

    public BigQueryTableHandle withProjectedColumns(List<BigQueryColumnHandle> newProjectedColumns)
    {
        return new BigQueryTableHandle(relationHandle, constraint, Optional.of(newProjectedColumns));
    }

    public enum BigQueryPartitionType
    {
        TIME,
        INGESTION,
        RANGE,
        /**/
    }

    public static Optional<BigQueryPartitionType> getPartitionType(TableDefinition definition)
    {
        if (definition instanceof StandardTableDefinition standardTableDefinition) {
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
