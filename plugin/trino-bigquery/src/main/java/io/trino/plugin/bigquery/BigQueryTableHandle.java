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
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record BigQueryTableHandle(
        BigQueryRelationHandle relationHandle,
        TupleDomain<ColumnHandle> constraint,
        Optional<List<BigQueryColumnHandle>> projectedColumns,
        OptionalLong limit)
        implements ConnectorTableHandle
{
    public BigQueryTableHandle
    {
        requireNonNull(relationHandle, "relationHandle is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(projectedColumns, "projectedColumns is null");
        requireNonNull(limit, "limit is null");
    }

    @JsonIgnore
    public BigQueryNamedRelationHandle getRequiredNamedRelation()
    {
        checkState(isNamedRelation(), "The table handle does not represent a named relation: %s", this);
        return (BigQueryNamedRelationHandle) relationHandle;
    }

    @JsonIgnore
    public BigQueryQueryRelationHandle getRequiredQueryRelation()
    {
        checkState(isQueryRelation(), "The table handle does not represent a query relation: %s", this);
        return (BigQueryQueryRelationHandle) relationHandle;
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

    @JsonIgnore
    public boolean isQueryRelation()
    {
        return relationHandle instanceof BigQueryQueryRelationHandle;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(relationHandle);
        if (constraint.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!constraint.isAll()) {
            builder.append(" constraint on ");
            builder.append(constraint.getDomains().orElseThrow().keySet().stream()
                    .map(columnHandle -> ((BigQueryColumnHandle) columnHandle).name())
                    .collect(joining(", ", "[", "]")));
        }
        projectedColumns.ifPresent(columns -> builder.append(" columns=").append(columns));
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        return builder.toString();
    }

    public BigQueryNamedRelationHandle asPlainTable()
    {
        checkState(!isSynthetic(), "The table handle does not represent a plain table: %s", this);
        return getRequiredNamedRelation();
    }

    BigQueryTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new BigQueryTableHandle(relationHandle, newConstraint, projectedColumns, limit);
    }

    public BigQueryTableHandle withProjectedColumns(List<BigQueryColumnHandle> newProjectedColumns)
    {
        return new BigQueryTableHandle(relationHandle, constraint, Optional.of(newProjectedColumns), limit);
    }

    public BigQueryTableHandle withLimit(long limit)
    {
        return new BigQueryTableHandle(relationHandle, constraint, projectedColumns, OptionalLong.of(limit));
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
