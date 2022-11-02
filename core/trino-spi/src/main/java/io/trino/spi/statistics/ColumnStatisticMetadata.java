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
package io.trino.spi.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Experimental;
import io.trino.spi.expression.FunctionName;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticMetadata
{
    private final String columnName;
    private final String connectorAggregationId;
    private final Optional<ColumnStatisticType> statisticType;
    private final Optional<FunctionName> aggregation;

    public ColumnStatisticMetadata(
            String columnName,
            ColumnStatisticType statisticType)
    {
        this(columnName, statisticType.name(), statisticType);
    }

    public ColumnStatisticMetadata(
            String columnName,
            String connectorAggregationId,
            ColumnStatisticType statisticType)
    {
        this(columnName, connectorAggregationId, Optional.of(statisticType), Optional.empty());
    }

    @Experimental(eta = "2023-01-31")
    public ColumnStatisticMetadata(
            String columnName,
            String connectorAggregationId,
            FunctionName aggregation)
    {
        this(columnName, connectorAggregationId, Optional.empty(), Optional.of(aggregation));
    }

    private ColumnStatisticMetadata(
            String columnName,
            String connectorAggregationId,
            Optional<ColumnStatisticType> statisticType,
            Optional<FunctionName> aggregation)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.connectorAggregationId = requireNonNull(connectorAggregationId, "connectorAggregationId is null");
        this.statisticType = requireNonNull(statisticType, "statisticType is null");
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
        if (statisticType.isPresent() == aggregation.isPresent()) {
            throw new IllegalArgumentException("Exactly one of statisticType and aggregation should be set");
        }
    }

    @Deprecated // For JSON deserialization only
    @JsonCreator
    public static ColumnStatisticMetadata fromJson(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("connectorAggregationId") String connectorAggregationId,
            @JsonProperty("statisticType") Optional<ColumnStatisticType> statisticType,
            @JsonProperty("aggregation") Optional<FunctionName> aggregation)
    {
        return new ColumnStatisticMetadata(columnName, connectorAggregationId, statisticType, aggregation);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @Experimental(eta = "2023-01-31")
    @JsonProperty
    public String getConnectorAggregationId()
    {
        return connectorAggregationId;
    }

    @JsonIgnore
    public ColumnStatisticType getStatisticType()
    {
        return statisticType.orElseThrow();
    }

    @Experimental(eta = "2023-01-31")
    @JsonProperty("statisticType")
    public Optional<ColumnStatisticType> getStatisticTypeIfPresent()
    {
        return statisticType;
    }

    @Experimental(eta = "2023-01-31")
    @JsonIgnore
    public FunctionName getAggregation()
    {
        return aggregation.orElseThrow();
    }

    @Experimental(eta = "2023-01-31")
    @JsonProperty("aggregation")
    public Optional<FunctionName> getAggregationIfPresent()
    {
        return aggregation;
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
        ColumnStatisticMetadata that = (ColumnStatisticMetadata) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(connectorAggregationId, that.connectorAggregationId) &&
                Objects.equals(statisticType, that.statisticType) &&
                Objects.equals(aggregation, that.aggregation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, connectorAggregationId, statisticType, aggregation);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ColumnStatisticMetadata.class.getSimpleName() + "[", "]")
                .add("columnName='" + columnName + "'")
                .add("connectorAggregationId='" + connectorAggregationId + "'")
                .add("statisticType=" + statisticType)
                .add("aggregation=" + aggregation)
                .toString();
    }
}
