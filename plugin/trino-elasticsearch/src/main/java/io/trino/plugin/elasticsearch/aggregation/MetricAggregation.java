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
package io.trino.plugin.elasticsearch.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public record MetricAggregation(
        @JsonProperty("functionName") String functionName,
        @JsonProperty("outputType") Type outputType,
        @JsonProperty("columnHandle") Optional<ElasticsearchColumnHandle> columnHandle,
        @JsonProperty("alias") String alias)
{
    public static final String MAX = "max";
    public static final String MIN = "min";
    public static final String AVG = "avg";
    public static final String SUM = "sum";
    public static final String COUNT = "count";
    private static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = ImmutableList.of(MAX, MIN, AVG, SUM, COUNT);
    private static final List<Type> NUMERIC_TYPES = ImmutableList.of(REAL, DOUBLE, TINYINT, SMALLINT, INTEGER, BIGINT);

    @JsonCreator
    public MetricAggregation
    {
        requireNonNull(functionName, "functionName is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(alias, "alias is null");
    }

    @JsonIgnore
    public boolean isCountAll()
    {
        return columnHandle.isEmpty() && COUNT.equals(functionName);
    }

    public static boolean isNumericType(Type type)
    {
        return NUMERIC_TYPES.contains(type);
    }

    private static boolean isValidAggregationColumn(String functionName, ElasticsearchColumnHandle column)
    {
        return switch (functionName) {
            // COUNT works on any column that supports predicates
            case COUNT -> true;
            // Elasticsearch calculates MIN/MAX in double, so BIGINT can lose precision.
            case MIN, MAX -> isNumericType(column.type()) && !BIGINT.equals(column.type());
            // SUM returns BIGINT for integral inputs, which can lose precision in Elasticsearch.
            case SUM -> REAL.equals(column.type()) || DOUBLE.equals(column.type());
            case AVG -> isNumericType(column.type());
            default -> false;
        };
    }

    public static Optional<MetricAggregation> handleAggregation(
            AggregateFunction function,
            Map<String, ColumnHandle> assignments,
            String alias)
    {
        if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty()) {
            return Optional.empty();
        }

        if (!SUPPORTED_AGGREGATION_FUNCTIONS.contains(function.getFunctionName())) {
            return Optional.empty();
        }

        // Special case: COUNT(*) has no arguments
        if (COUNT.equals(function.getFunctionName()) && function.getArguments().isEmpty()) {
            return Optional.of(new MetricAggregation(COUNT, function.getOutputType(), Optional.empty(), alias));
        }

        Optional<ElasticsearchColumnHandle> parameterColumnHandle = function.getArguments().stream()
                .filter(input -> input instanceof Variable)
                .map(Variable.class::cast)
                .map(Variable::getName)
                .filter(assignments::containsKey)
                .findFirst()
                .map(assignments::get)
                .map(ElasticsearchColumnHandle.class::cast)
                .filter(ElasticsearchColumnHandle::supportsPredicates)
                .filter(ElasticsearchColumnHandle::supportsTopNAndAggregations)
                .filter(column -> isValidAggregationColumn(function.getFunctionName(), column));
        if (parameterColumnHandle.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MetricAggregation(function.getFunctionName(), function.getOutputType(), parameterColumnHandle, alias));
    }
}
