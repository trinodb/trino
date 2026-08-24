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
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;

/**
 * Converts connector-owned legacy predicate state into the canonical remote predicate IR.
 *
 * <p>This class is intentionally independent from SQL expression recognition. It is also used by dynamic filtering,
 * where predicates are generated at execution time instead of by metadata pushdown.</p>
 */
final class ElasticsearchRemotePredicateTranslator
{
    private ElasticsearchRemotePredicateTranslator() {}

    public static ElasticsearchTableHandle canonicalize(ElasticsearchTableHandle table, Optional<ElasticsearchRemotePredicate> inheritedPredicate)
    {
        requireNonNull(table, "table is null");
        requireNonNull(inheritedPredicate, "inheritedPredicate is null");

        if (table.constraint().isNone()) {
            return withRemotePredicate(table, combine(inheritedPredicate, table.remotePredicate()));
        }

        List<ElasticsearchRemotePredicate> predicates = new ArrayList<>();
        inheritedPredicate.ifPresent(predicates::add);
        table.remotePredicate().ifPresent(predicates::add);
        translateConstraint(table.constraint().transformKeys(ElasticsearchColumnHandle.class::cast)).ifPresent(predicates::add);
        table.regexes().forEach((field, value) -> predicates.add(new ElasticsearchRemotePredicate.Regexp(field, value)));
        table.prefixes().forEach((field, value) -> predicates.add(new ElasticsearchRemotePredicate.Prefix(field, value)));
        table.matchPhrasePrefixes().forEach((field, value) -> predicates.add(new ElasticsearchRemotePredicate.MatchPhrasePrefix(field, value)));

        return new ElasticsearchTableHandle(
                table.type(),
                table.schema(),
                table.index(),
                TupleDomain.all(),
                Map.of(),
                Map.of(),
                Map.of(),
                table.query(),
                table.limit(),
                table.sortOrder(),
                table.columns(),
                table.aggregation(),
                conjunction(predicates));
    }

    public static Optional<ElasticsearchRemotePredicate> translateConstraint(TupleDomain<ElasticsearchColumnHandle> constraint)
    {
        requireNonNull(constraint, "constraint is null");
        if (constraint.isAll() || constraint.isNone()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ElasticsearchRemotePredicate> predicates = ImmutableList.builder();
        for (Entry<ElasticsearchColumnHandle, Domain> entry : constraint.getDomains().orElseThrow().entrySet()) {
            translateDomain(entry.getKey(), entry.getValue()).ifPresent(predicates::add);
        }
        return conjunction(predicates.build());
    }

    public static Optional<ElasticsearchRemotePredicate> translateDomain(ElasticsearchColumnHandle column, Domain domain)
    {
        requireNonNull(column, "column is null");
        requireNonNull(domain, "domain is null");
        if (domain.isAll()) {
            return Optional.empty();
        }

        String field = column.predicateName();
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return Optional.of(new ElasticsearchRemotePredicate.Not(new ElasticsearchRemotePredicate.Exists(field)));
            }
            return conjunction(List.of(
                    new ElasticsearchRemotePredicate.Exists(field),
                    new ElasticsearchRemotePredicate.Not(new ElasticsearchRemotePredicate.Exists(field))));
        }
        if (domain.getValues().isAll()) {
            return Optional.of(new ElasticsearchRemotePredicate.Exists(field));
        }

        List<ElasticsearchRemotePredicate> alternatives = new ArrayList<>();
        if (!column.supportsPredicates() && column.type() instanceof VarcharType) {
            if (!domain.getValues().isDiscreteSet()) {
                return Optional.empty();
            }
            for (Object value : domain.getValues().getDiscreteSet()) {
                alternatives.add(new ElasticsearchRemotePredicate.MatchPhrase(field, ((Slice) value).toStringUtf8()));
            }
        }
        else if (domain.getValues().isDiscreteSet()) {
            List<Object> values = domain.getValues().getDiscreteSet().stream()
                    .map(value -> getValue(column.type(), value))
                    .toList();
            if (values.size() == 1) {
                alternatives.add(new ElasticsearchRemotePredicate.Term(field, values.getFirst()));
            }
            else if (!values.isEmpty()) {
                alternatives.add(new ElasticsearchRemotePredicate.Terms(field, values));
            }
        }
        else if (domain.getValues().complement().isDiscreteSet()) {
            List<Object> excludedValues = domain.getValues().complement().getDiscreteSet().stream()
                    .map(value -> getValue(column.type(), value))
                    .toList();
            ElasticsearchRemotePredicate excluded = excludedValues.size() == 1
                    ? new ElasticsearchRemotePredicate.Term(field, excludedValues.getFirst())
                    : new ElasticsearchRemotePredicate.Terms(field, excludedValues);
            List<ElasticsearchRemotePredicate> predicates = new ArrayList<>();
            if (!domain.isNullAllowed()) {
                predicates.add(new ElasticsearchRemotePredicate.Exists(field));
            }
            predicates.add(new ElasticsearchRemotePredicate.Not(excluded));
            return conjunction(predicates);
        }
        else {
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    alternatives.add(new ElasticsearchRemotePredicate.Term(field, getValue(column.type(), range.getSingleValue())));
                    continue;
                }
                Optional<ElasticsearchRemotePredicate.Bound> lower = range.isLowUnbounded()
                        ? Optional.empty()
                        : Optional.of(new ElasticsearchRemotePredicate.Bound(getValue(column.type(), range.getLowBoundedValue()), range.isLowInclusive()));
                Optional<ElasticsearchRemotePredicate.Bound> upper = range.isHighUnbounded()
                        ? Optional.empty()
                        : Optional.of(new ElasticsearchRemotePredicate.Bound(getValue(column.type(), range.getHighBoundedValue()), range.isHighInclusive()));
                if (lower.isPresent() || upper.isPresent()) {
                    alternatives.add(new ElasticsearchRemotePredicate.Range(field, lower, upper));
                }
            }
        }

        if (domain.isNullAllowed()) {
            alternatives.add(new ElasticsearchRemotePredicate.Not(new ElasticsearchRemotePredicate.Exists(field)));
        }
        return disjunction(alternatives);
    }

    public static Optional<ElasticsearchRemotePredicate> combine(
            Optional<ElasticsearchRemotePredicate> left,
            Optional<ElasticsearchRemotePredicate> right)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return conjunction(List.of(left.orElseThrow(), right.orElseThrow()));
    }

    public static Optional<ElasticsearchRemotePredicate> conjunction(List<ElasticsearchRemotePredicate> predicates)
    {
        requireNonNull(predicates, "predicates is null");
        List<ElasticsearchRemotePredicate> flattened = new ArrayList<>();
        predicates.forEach(predicate -> addConjunct(flattened, predicate));
        if (flattened.isEmpty()) {
            return Optional.empty();
        }
        if (flattened.size() == 1) {
            return Optional.of(flattened.getFirst());
        }
        return Optional.of(new ElasticsearchRemotePredicate.And(flattened));
    }

    private static void addConjunct(List<ElasticsearchRemotePredicate> conjuncts, ElasticsearchRemotePredicate predicate)
    {
        if (predicate instanceof ElasticsearchRemotePredicate.And and) {
            and.predicates().forEach(conjunct -> addConjunct(conjuncts, conjunct));
            return;
        }
        if (!conjuncts.contains(predicate)) {
            conjuncts.add(predicate);
        }
    }

    public static Optional<ElasticsearchRemotePredicate> disjunction(List<ElasticsearchRemotePredicate> predicates)
    {
        requireNonNull(predicates, "predicates is null");
        List<ElasticsearchRemotePredicate> flattened = predicates.stream()
                .flatMap(predicate -> predicate instanceof ElasticsearchRemotePredicate.Or or ? or.predicates().stream() : Stream.of(predicate))
                .toList();
        if (flattened.isEmpty()) {
            return Optional.empty();
        }
        if (flattened.size() == 1) {
            return Optional.of(flattened.getFirst());
        }
        return Optional.of(new ElasticsearchRemotePredicate.Or(flattened));
    }

    public static ElasticsearchTableHandle withRemotePredicate(ElasticsearchTableHandle table, Optional<ElasticsearchRemotePredicate> predicate)
    {
        return new ElasticsearchTableHandle(
                table.type(),
                table.schema(),
                table.index(),
                table.constraint(),
                table.regexes(),
                table.prefixes(),
                table.matchPhrasePrefixes(),
                table.query(),
                table.limit(),
                table.sortOrder(),
                table.columns(),
                table.aggregation(),
                predicate);
    }

    public static Object getValue(Type type, Object value)
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
            return Float.intBitsToFloat(toIntExact((Long) value));
        }
        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return Instant.ofEpochMilli(floorDiv((Long) value, MICROSECONDS_PER_MILLISECOND))
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime()
                    .format(ISO_DATE_TIME);
        }
        if (type.getBaseName().equalsIgnoreCase("ipaddress") && value instanceof Slice slice) {
            try {
                return InetAddress.getByAddress(slice.getBytes()).getHostAddress();
            }
            catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid IP address value", e);
            }
        }
        throw new IllegalArgumentException("Unhandled remote predicate type: " + type);
    }
}
