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
package io.trino.plugin.hive.s3select;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.s3select.S3SelectDataType.CSV;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;
import static org.joda.time.format.ISODateTimeFormat.date;

/**
 * S3 Select uses Ion SQL++ query language. This class is used to construct a valid Ion SQL++ query
 * to be evaluated with S3 Select on an S3 object.
 */
public class IonSqlQueryBuilder
{
    private static final DateTimeFormatter FORMATTER = date().withChronology(getInstanceUTC());
    private static final String DATA_SOURCE = "S3Object s";
    private final TypeManager typeManager;
    private final S3SelectDataType s3SelectDataType;
    private final String nullPredicate;
    private final String notNullPredicate;

    public IonSqlQueryBuilder(TypeManager typeManager, S3SelectDataType s3SelectDataType, Optional<String> optionalNullCharacterEncoding)
    {
        if (optionalNullCharacterEncoding.isPresent()) {
            checkArgument(s3SelectDataType == CSV, "Null character encoding should only be provided for CSV data");
        }

        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.s3SelectDataType = requireNonNull(s3SelectDataType, "s3SelectDataType is null");

        String nullCharacterEncoding = optionalNullCharacterEncoding.orElse("");
        this.nullPredicate = switch (s3SelectDataType) {
            case JSON -> "IS NULL";
            case CSV -> "= '%s'".formatted(nullCharacterEncoding);
        };
        this.notNullPredicate = switch (s3SelectDataType) {
            case JSON -> "IS NOT NULL";
            case CSV -> "!= '%s'".formatted(nullCharacterEncoding);
        };
    }

    public String buildSql(List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        columns.forEach(column -> checkArgument(column.isBaseColumn(), "%s is not a base column", column));
        tupleDomain.getDomains().ifPresent(domains -> {
            domains.keySet().forEach(column -> checkArgument(column.isBaseColumn(), "%s is not a base column", column));
        });

        // SELECT clause
        StringBuilder sql = new StringBuilder("SELECT ");

        if (columns.isEmpty()) {
            sql.append("' '");
        }
        else {
            String columnNames = columns.stream()
                    .map(this::getFullyQualifiedColumnName)
                    .collect(joining(", "));
            sql.append(columnNames);
        }

        // FROM clause
        sql.append(" FROM ");
        sql.append(DATA_SOURCE);

        // WHERE clause
        List<String> clauses = toConjuncts(columns, tupleDomain);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        return sql.toString();
    }

    private String getFullyQualifiedColumnName(HiveColumnHandle column)
    {
        return switch (s3SelectDataType) {
            case JSON -> "s.%s".formatted(column.getBaseColumnName());
            case CSV -> "s._%d".formatted(column.getBaseHiveColumnIndex() + 1);
        };
    }

    private List<String> toConjuncts(List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            Type type = column.getHiveType().getType(typeManager);
            if (tupleDomain.getDomains().isPresent() && isSupported(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(domain, type, column));
                }
            }
        }
        return builder.build();
    }

    private static boolean isSupported(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BIGINT) ||
                validType.equals(TINYINT) ||
                validType.equals(SMALLINT) ||
                validType.equals(INTEGER) ||
                validType.equals(BOOLEAN) ||
                validType.equals(DATE) ||
                validType instanceof VarcharType;
    }

    private String toPredicate(Domain domain, Type type, HiveColumnHandle column)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return getFullyQualifiedColumnName(column) + " " + nullPredicate;
            }
            return "FALSE";
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                return "TRUE";
            }
            return getFullyQualifiedColumnName(column) + " " + notNullPredicate;
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll());
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
                continue;
            }
            List<String> rangeConjuncts = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                rangeConjuncts.add(toPredicate(range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), type, column));
            }
            if (!range.isHighUnbounded()) {
                rangeConjuncts.add(toPredicate(range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), type, column));
            }
            // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
            checkState(!rangeConjuncts.isEmpty());
            if (rangeConjuncts.size() == 1) {
                disjuncts.add("%s %s AND %s".formatted(getFullyQualifiedColumnName(column), notNullPredicate, getOnlyElement(rangeConjuncts)));
            }
            else {
                disjuncts.add("(%s %s AND %s)".formatted(getFullyQualifiedColumnName(column), notNullPredicate, Joiner.on(" AND ").join(rangeConjuncts)));
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add("%s %s AND %s".formatted(getFullyQualifiedColumnName(column), notNullPredicate, toPredicate("=", getOnlyElement(singleValues), type, column)));
        }
        else if (singleValues.size() > 1) {
            List<String> values = new ArrayList<>();
            for (Object value : singleValues) {
                checkType(type);
                values.add(valueToQuery(type, value));
            }
            disjuncts.add("%s %s AND %s IN (%s)".formatted(
                    getFullyQualifiedColumnName(column),
                    notNullPredicate,
                    createColumn(type, column),
                    Joiner.on(",").join(values)));
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(getFullyQualifiedColumnName(column) + " " + nullPredicate);
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String operator, Object value, Type type, HiveColumnHandle column)
    {
        checkType(type);

        return format("%s %s %s", createColumn(type, column), operator, valueToQuery(type, value));
    }

    private static void checkType(Type type)
    {
        checkArgument(isSupported(type), "Type not supported: %s", type);
    }

    private static String valueToQuery(Type type, Object value)
    {
        if (type.equals(BIGINT)) {
            return String.valueOf((long) value);
        }
        if (type.equals(INTEGER)) {
            return String.valueOf(toIntExact((long) value));
        }
        if (type.equals(SMALLINT)) {
            return String.valueOf(Shorts.checkedCast((long) value));
        }
        if (type.equals(TINYINT)) {
            return String.valueOf(SignedBytes.checkedCast((long) value));
        }
        if (type.equals(BOOLEAN)) {
            return String.valueOf((boolean) value);
        }
        if (type.equals(DATE)) {
            // CAST('2007-04-05T14:30Z' AS TIMESTAMP)
            return "'" + FORMATTER.print(DAYS.toMillis((long) value)) + "'";
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }
        return "'" + ((Slice) value).toStringUtf8() + "'";
    }

    private String createColumn(Type type, HiveColumnHandle columnHandle)
    {
        String column = getFullyQualifiedColumnName(columnHandle);

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return "CAST(" + column + " AS INT)";
        }
        if (type.equals(BOOLEAN)) {
            return "CAST(" + column + " AS BOOL)";
        }
        return column;
    }
}
