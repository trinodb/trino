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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public final class ElasticsearchQueryBuilder
{
    private ElasticsearchQueryBuilder() {}

    public static QueryBuilder buildSearchQuery(TupleDomain<ElasticsearchColumnHandle> constraint, Optional<String> query, Map<String, String> regexes)
    {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (constraint.getDomains().isPresent()) {
            for (Map.Entry<ElasticsearchColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
                ElasticsearchColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();

                checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.getName());
                if (!domain.isAll()) {
                    addPredicateToQueryBuilder(queryBuilder, column.getName(), domain, column.getType());
                }
            }
        }

        regexes.forEach((name, value) -> queryBuilder.filter(new BoolQueryBuilder().must(((new RegexpQueryBuilder(name, value))))));

        query.map(QueryStringQueryBuilder::new)
                .ifPresent(queryBuilder::must);

        if (queryBuilder.hasClauses()) {
            return queryBuilder;
        }
        return new MatchAllQueryBuilder();
    }

    private static void addPredicateToQueryBuilder(BoolQueryBuilder queryBuilder, String columnName, Domain domain, Type type)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            queryBuilder.mustNot(new ExistsQueryBuilder(columnName));
            return;
        }

        if (domain.getValues().isAll()) {
            queryBuilder.filter(new ExistsQueryBuilder(columnName));
            return;
        }

        List<QueryBuilder> shouldClauses = getShouldClauses(columnName, domain, type);
        if (shouldClauses.size() == 1) {
            queryBuilder.filter(getOnlyElement(shouldClauses));
            return;
        }
        if (shouldClauses.size() > 1) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            shouldClauses.forEach(boolQueryBuilder::should);
            queryBuilder.filter(boolQueryBuilder);
            return;
        }
    }

    private static List<QueryBuilder> getShouldClauses(String columnName, Domain domain, Type type)
    {
        ImmutableList.Builder<QueryBuilder> shouldClauses = ImmutableList.builder();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll(), "Invalid range for column: %s", columnName);
            if (range.isSingleValue()) {
                shouldClauses.add(new TermQueryBuilder(columnName, getValue(type, range.getSingleValue())));
            }
            else {
                RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(columnName);
                if (!range.isLowUnbounded()) {
                    Object lowBound = getValue(type, range.getLowBoundedValue());
                    if (range.isLowInclusive()) {
                        rangeQueryBuilder.gte(lowBound);
                    }
                    else {
                        rangeQueryBuilder.gt(lowBound);
                    }
                }
                if (!range.isHighUnbounded()) {
                    Object highBound = getValue(type, range.getHighBoundedValue());
                    if (range.isHighInclusive()) {
                        rangeQueryBuilder.lte(highBound);
                    }
                    else {
                        rangeQueryBuilder.lt(highBound);
                    }
                }
                shouldClauses.add(rangeQueryBuilder);
            }
        }
        if (domain.isNullAllowed()) {
            shouldClauses.add(new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(columnName)));
        }
        return shouldClauses.build();
    }

    private static Object getValue(Type type, Object value)
    {
        if (type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(DOUBLE)) {
            return value;
        }
        if (type.equals(REAL)) {
            return Float.intBitsToFloat(toIntExact(((Long) value)));
        }
        if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return Instant.ofEpochMilli(floorDiv((Long) value, MICROSECONDS_PER_MILLISECOND))
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime()
                    .format(ISO_DATE_TIME);
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }
}
