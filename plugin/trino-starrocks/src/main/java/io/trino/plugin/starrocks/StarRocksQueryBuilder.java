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
package io.trino.plugin.starrocks;

import io.airlift.slice.Slice;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class StarRocksQueryBuilder
{
    private static final int MAX_PUSHDOWN_DISCRETE_VALUES = 100;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    public String buildSelectSql(StarRocksTableHandle tableHandle, List<StarRocksColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");

        StringBuilder sql = new StringBuilder("SELECT ")
                .append(buildProjection(tableHandle, columns))
                .append(" FROM ")
                .append(qualifiedTableName(tableHandle));

        buildWhereClause(tableHandle.constraint()).ifPresent(where -> sql.append(" WHERE ").append(where));
        buildGroupByClause(tableHandle.aggregation()).ifPresent(groupBy -> sql.append(" GROUP BY ").append(groupBy));
        buildOrderByClause(tableHandle.sortOrder()).ifPresent(orderBy -> sql.append(" ORDER BY ").append(orderBy));
        tableHandle.limit().ifPresent(limit -> sql.append(" LIMIT ").append(limit));
        return sql.toString();
    }

    static String quoteIdentifier(String value)
    {
        return "`" + value.replace("`", "``") + "`";
    }

    static Optional<String> buildWhereClause(TupleDomain<StarRocksColumnHandle> constraint)
    {
        requireNonNull(constraint, "constraint is null");
        if (constraint.isNone()) {
            return Optional.of("1 = 0");
        }
        if (constraint.isAll()) {
            return Optional.empty();
        }

        List<String> predicates = new ArrayList<>();
        for (Map.Entry<StarRocksColumnHandle, Domain> entry : constraint.getDomains().orElseThrow().entrySet()) {
            buildColumnPredicate(entry.getKey(), entry.getValue()).ifPresent(predicates::add);
        }
        if (predicates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(predicates.stream()
                .map(predicate -> "(" + predicate + ")")
                .collect(joining(" AND ")));
    }

    static Optional<String> buildColumnPredicate(StarRocksColumnHandle column, Domain domain)
    {
        requireNonNull(column, "column is null");
        requireNonNull(domain, "domain is null");
        try {
            return buildColumnPredicateUnchecked(column, domain);
        }
        catch (IllegalArgumentException _) {
            return Optional.empty();
        }
    }

    private static Optional<String> buildColumnPredicateUnchecked(StarRocksColumnHandle column, Domain domain)
    {
        String columnExpression = quoteIdentifier(column.remoteColumnName());
        Type type = column.columnType();
        if (domain.isNone()) {
            return Optional.of("1 = 0");
        }
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isOnlyNull()) {
            return Optional.of(columnExpression + " IS NULL");
        }

        ValueSet values = domain.getValues();
        if (values.isAll()) {
            if (domain.isNullAllowed()) {
                return Optional.empty();
            }
            return Optional.of(columnExpression + " IS NOT NULL");
        }
        if (values.isDiscreteSet() && isDiscretePredicatePushdownSupported(type)) {
            List<Object> discreteValues = values.getDiscreteSet();
            if (discreteValues.size() > MAX_PUSHDOWN_DISCRETE_VALUES) {
                return Optional.empty();
            }
            String valuesPredicate = discreteValues.size() == 1
                    ? columnExpression + " = " + toSqlLiteral(type, discreteValues.getFirst())
                    : columnExpression + " IN (" + discreteValues.stream()
                                                   .map(value -> toSqlLiteral(type, value))
                                                   .collect(joining(", ")) + ")";
            if (domain.isNullAllowed()) {
                return Optional.of("(" + valuesPredicate + " OR " + columnExpression + " IS NULL)");
            }
            return Optional.of(valuesPredicate);
        }
        if (isRangePredicatePushdownSupported(type)) {
            List<String> rangePredicates = values.getRanges().getOrderedRanges().stream()
                    .map(range -> buildRangePredicate(columnExpression, type, range))
                    .toList();
            if (rangePredicates.isEmpty()) {
                return domain.isNullAllowed() ? Optional.of(columnExpression + " IS NULL") : Optional.empty();
            }

            String predicate = rangePredicates.size() == 1
                    ? rangePredicates.getFirst()
                    : rangePredicates.stream()
                      .map(value -> "(" + value + ")")
                      .collect(joining(" OR "));
            if (domain.isNullAllowed()) {
                predicate = predicate + " OR " + columnExpression + " IS NULL";
            }
            return Optional.of(predicate);
        }
        return Optional.empty();
    }

    private static String buildProjection(StarRocksTableHandle tableHandle, List<StarRocksColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return "1";
        }
        if (tableHandle.aggregation().isPresent()) {
            Map<String, StarRocksAggregateColumn> aggregateColumns = tableHandle.aggregation().orElseThrow().aggregateColumns().stream()
                    .collect(toMap(StarRocksAggregateColumn::columnName, identity()));
            return columns.stream()
                    .map(column -> aggregateColumns.containsKey(column.columnName())
                            ? aggregateColumns.get(column.columnName()).expression() + " AS " + quoteIdentifier(column.columnName())
                            : quoteIdentifier(column.remoteColumnName()) + " AS " + quoteIdentifier(column.columnName()))
                    .collect(joining(", "));
        }
        return columns.stream()
                .map(column -> quoteIdentifier(column.remoteColumnName()) + " AS " + quoteIdentifier(column.columnName()))
                .collect(joining(", "));
    }

    private static String qualifiedTableName(StarRocksTableHandle tableHandle)
    {
        return tableHandle.remoteCatalogName()
                .map(catalog -> quoteIdentifier(catalog) + ".")
                .orElse("") +
                quoteIdentifier(tableHandle.remoteSchemaName()) +
                "." +
                quoteIdentifier(tableHandle.remoteTableName());
    }

    private static Optional<String> buildGroupByClause(Optional<StarRocksAggregation> aggregation)
    {
        if (aggregation.isEmpty() || aggregation.orElseThrow().groupingColumns().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(aggregation.orElseThrow().groupingColumns().stream()
                .map(StarRocksColumnHandle::remoteColumnName)
                .map(StarRocksQueryBuilder::quoteIdentifier)
                .collect(joining(", ")));
    }

    private static Optional<String> buildOrderByClause(List<StarRocksSortItem> sortOrder)
    {
        if (sortOrder.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(sortOrder.stream()
                .map(StarRocksQueryBuilder::toOrderByExpression)
                .collect(joining(", ")));
    }

    private static String toOrderByExpression(StarRocksSortItem sortItem)
    {
        String column = quoteIdentifier(sortItem.remoteColumnName());
        SortOrder sortOrder = sortItem.sortOrder();
        String nullOrdering = sortOrder.isNullsFirst()
                ? column + " IS NOT NULL ASC"
                : column + " IS NULL ASC";
        return nullOrdering + ", " + column + (sortOrder.isAscending() ? " ASC" : " DESC");
    }

    private static String buildRangePredicate(String columnExpression, Type type, Range range)
    {
        if (range.isSingleValue()) {
            return columnExpression + " = " + toSqlLiteral(type, range.getSingleValue());
        }

        List<String> predicates = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            predicates.add(columnExpression + (range.isLowInclusive() ? " >= " : " > ") + toSqlLiteral(type, range.getLowBoundedValue()));
        }
        if (!range.isHighUnbounded()) {
            predicates.add(columnExpression + (range.isHighInclusive() ? " <= " : " < ") + toSqlLiteral(type, range.getHighBoundedValue()));
        }
        return predicates.stream().collect(joining(" AND "));
    }

    private static boolean isDiscretePredicatePushdownSupported(Type type)
    {
        return type == BOOLEAN ||
                type == TINYINT ||
                type == SMALLINT ||
                type == INTEGER ||
                type == BIGINT ||
                type == REAL ||
                type == DOUBLE ||
                type == DATE ||
                type instanceof DecimalType ||
                type instanceof TimestampType ||
                type instanceof VarcharType ||
                type instanceof CharType;
    }

    private static boolean isRangePredicatePushdownSupported(Type type)
    {
        return type == TINYINT ||
                type == SMALLINT ||
                type == INTEGER ||
                type == BIGINT ||
                type == REAL ||
                type == DOUBLE ||
                type == DATE ||
                type instanceof DecimalType ||
                type instanceof TimestampType;
    }

    private static String toSqlLiteral(Type type, Object value)
    {
        if (value == null) {
            return "NULL";
        }
        if (type == BOOLEAN) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return value.toString();
        }
        if (type == REAL) {
            float realValue = Float.intBitsToFloat(((Number) value).intValue());
            if (!Float.isFinite(realValue)) {
                throw new IllegalArgumentException("Unsupported StarRocks REAL literal: " + realValue);
            }
            return Float.toString(realValue);
        }
        if (type == DOUBLE) {
            double doubleValue = ((Number) value).doubleValue();
            if (!Double.isFinite(doubleValue)) {
                throw new IllegalArgumentException("Unsupported StarRocks DOUBLE literal: " + doubleValue);
            }
            return Double.toString(doubleValue);
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return Decimals.toString((long) value, decimalType.getScale());
            }
            return Decimals.toString((Int128) value, decimalType.getScale());
        }
        if (type == DATE) {
            return "DATE " + quoteStringLiteral(LocalDate.ofEpochDay((long) value).toString());
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            long epochMicros = (long) value;
            long epochSeconds = Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            long microsOfSecond = Math.floorMod(epochMicros, MICROSECONDS_PER_SECOND);
            LocalDateTime timestamp = Instant.ofEpochSecond(epochSeconds, microsOfSecond * 1_000)
                    .atZone(UTC)
                    .toLocalDateTime();
            return "TIMESTAMP " + quoteStringLiteral(timestamp.format(TIMESTAMP_FORMATTER));
        }
        if (type instanceof VarcharType || type instanceof CharType || type.getBaseName().equals(JSON)) {
            return quoteStringLiteral(toTextValue(value));
        }
        throw new IllegalArgumentException("Unsupported StarRocks predicate literal type: " + type);
    }

    private static String toTextValue(Object value)
    {
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        return value.toString();
    }

    private static String quoteStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
