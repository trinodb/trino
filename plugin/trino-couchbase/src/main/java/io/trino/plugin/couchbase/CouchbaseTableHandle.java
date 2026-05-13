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
package io.trino.plugin.couchbase;

import com.google.common.collect.Streams;
import io.airlift.slice.Slice;
import io.trino.plugin.couchbase.translations.TrinoExpressionToCb;
import io.trino.plugin.couchbase.translations.TrinoToCbType;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public record CouchbaseTableHandle(String schema, String name, Optional<CouchbaseTableHandle> subQuery,
                                   List<NamedParametrizedString> selectClauses, List<Type> selectTypes,
                                   List<String> selectNames, List<ParametrizedString> whereClauses,
                                   TupleDomain<ColumnHandle> constraint,
                                   LinkedHashMap<String, CouchbaseColumnHandle> orderClauses,
                                   Set<CouchbaseColumnHandle> groupings,
                                   AtomicBoolean isAggregated,
                                   AtomicLong topNCount) implements ConnectorTableHandle
{
    public String path()
    {
        return String.format("`%s`.`%s`", schema, name);
    }

    public static CouchbaseTableHandle fromSchemaAndName(String schema, String name)
    {
        return new CouchbaseTableHandle(
                schema,
                name,
                Optional.empty(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                TupleDomain.all(),
                new LinkedHashMap<>(),
                new HashSet<>(),
                new AtomicBoolean(false),
                new AtomicLong(-1L));
    }

    public void addSortItems(List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        CouchbaseTableHandle sq = subQuery.orElse(null);
        List<SortItem> pushdown =  new ArrayList<>();
        sortItems.forEach(sortItem -> {
            CouchbaseColumnHandle sourceColumn = (CouchbaseColumnHandle) assignments.get(sortItem.getName());
            orderClauses.put(transformSortItem(sortItem, assignments), sourceColumn);
        });
        if (!pushdown.isEmpty()) {
            sq.addSortItems(pushdown, assignments);
        }
    }

    protected String transformSortItem(SortItem sortItem, Map<String, ColumnHandle> assignments)
    {
        CouchbaseColumnHandle column = (CouchbaseColumnHandle) assignments.get(sortItem.getName());
        return String.format("%s %s", column.name(), sortItem.getSortOrder().toString());
    }

    public boolean compareSortItems(List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        if (this.orderClauses.size() != sortItems.size()) {
            return false;
        }
        return sortItems.stream().map(si -> transformSortItem(si, assignments)).allMatch(orderClauses.keySet()::contains);
    }

    public boolean hasVariable(String name)
    {
        return selectClauses.stream()
                .anyMatch(c -> {
                    if (c.name() == null) {
                        String text = c.value().text();
                        return c.value().params().isEmpty() &&
                                text.startsWith(name, 1) &&
                                text.length() == name.length() + 2 &&
                                text.endsWith("`");
                    }
                    return c.name().equals(name);
                });
    }

    public void setTopNCount(long topNCount)
    {
        this.topNCount.set(topNCount);
    }

    public List<String> addProjections(List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return projections.stream().map(p -> addProjection(p, assignments)).toList();
    }

    private String addProjection(ConnectorExpression projection, Map<String, ColumnHandle> assignments)
    {
        NamedParametrizedString compiled = compileProjection(projection, assignments);
        if (!selectClauses.contains(compiled)) {
            String otherName = findName(compiled.value()).orElse(null);
            if (otherName == null) {
                if (compiled.name() == null) {
                    compiled = new NamedParametrizedString(generateColumnName(), compiled.value());
                } else if (hasVariable(compiled.name())) {
                    return compiled.name();
                }
                selectClauses.add(compiled);
                selectTypes.add(projection.getType());
                selectNames.add(compiled.name());
            }
            else {
                return otherName;
            }
        }
        return compiled.name();
    }

    private Optional<String> findName(ParametrizedString value)
    {
        return selectClauses.stream().filter(nps -> nps.value().equals(value)).findFirst().map(nps -> nps.name());
    }

    private NamedParametrizedString compileProjection(ConnectorExpression projection, Map<String, ColumnHandle> assignments)
    {
        if (projection instanceof Variable variable) {
            ParametrizedString compiled = TrinoExpressionToCb.convert(projection, assignments);
            return new NamedParametrizedString(variable.getName(), compiled);
        }
        else {
            ParametrizedString compiled = TrinoExpressionToCb.convert(projection, assignments);
            return new NamedParametrizedString(null, compiled);
        }
    }

    public String toSql(String offsetId)
    {
        List<String> fromClause = new ArrayList<>();
        boolean fromSubQuery = false;
        if (subQuery.isPresent()) {
            CouchbaseTableHandle sq = subQuery.get();
            if (this.topNCount.get() < 0 && this.whereClauses().isEmpty() && this.orderClauses.isEmpty() && sq.selectClauses().containsAll(this.selectClauses) && this.groupings.isEmpty()) {
                return sq.toSql(offsetId);
            }
            if (sq != this && (sq.schema().equals(schema) && sq.name().equals(name))) {
                fromClause.add(String.format("(%s) `%s`", sq.toSql(offsetId), "data"));
//                    selectClauses.add(new NamedParametrizedString("data", ParametrizedString.from(String.format("`%s`.*", "data"))));
                fromSubQuery = true;
            }
        }
        if (fromClause.isEmpty()) {
            fromClause.add(String.format("`%s`", name));
        }

        StringBuilder groupByClause = new StringBuilder();
        if (!groupings.isEmpty()) {
            groupByClause.append(groupings.stream()
                    .map(CouchbaseColumnHandle::name)
                    .collect(Collectors.joining("`, `", " GROUP BY `", "`"))
            );
        }

        StringBuilder orderByClause = new StringBuilder();
        if (!orderClauses.isEmpty()) {
            orderByClause.append(String.format(" ORDER BY %s", String.join(", ", orderClauses.keySet())));
            if (!fromSubQuery) {
                orderByClause.append(", META().id");
            }
        } else if (!fromSubQuery) {
            orderByClause.append(" ORDER BY META().id");
        }

        StringBuilder whereClause = new StringBuilder();
        if (!whereClauses.isEmpty()) {
            whereClause.append(String.format(" WHERE %s",
                    whereClauses.stream().map(ParametrizedString::toString).collect(joining(" AND "))));
        } else {
            whereClause.append(" WHERE TRUE");
        }
        if (offsetId != null) {
            whereClause.append(String.format(" AND META().id > '%s'", offsetId));
        }

        String query = String.format("SELECT %s FROM %s%s%s%s",
                selectClauses.isEmpty() ? String.format("`%s`.*", subQuery.isPresent() ? "data" : name()):
                        selectClauses.stream().map(NamedParametrizedString::toString).collect(joining(", ")),
                String.join(", ", fromClause),
                whereClause.toString(),
                groupByClause.toString(),
                orderByClause.toString());

        if (topNCount.get() > -1) {
            query = String.format("%s LIMIT %d", query, topNCount.get());
        }

        return query;
    }

    private Stream<ParametrizedString> getParametrizedStrings()
    {
        return Streams.concat(
                selectClauses.stream().map(NamedParametrizedString::value),
                subQuery.stream().flatMap(CouchbaseTableHandle::getParametrizedStrings),
                whereClauses.stream());
    }

    public List<Object> getParameters()
    {
        return getParametrizedStrings().flatMap(p -> p.params().stream()).collect(Collectors.toList());
    }

    public boolean isEmpty()
    {
        return (subQuery.isEmpty() || (subQuery.get() != this && subQuery.get().isEmpty())) &&
                topNCount.get() == -1 && selectClauses.isEmpty() && whereClauses.isEmpty() && orderClauses.isEmpty() && groupings.isEmpty();
    }

    private ParametrizedString compileDomain(String left, Domain domain)
    {
        if (domain.isOnlyNull()) {
            return ParametrizedString.from(String.format("%s IS NULL", left));
        }
        else if (domain.isSingleValue()) {
            return ParametrizedString.from(String.format("%s = ?", left), Arrays.asList(TrinoToCbType.serialize(domain.getType(), domain.getSingleValue())));
        }
        else if (domain.getValues() instanceof SortedRangeSet rangeSet) {
            List<ParametrizedString> ranges = new ArrayList<>();
            if (rangeSet.isDiscreteSet()) {
                List<Object> values = rangeSet.getDiscreteSet();
                List<Object> include = new ArrayList<>();
                List<Object> exclude = new ArrayList<>();
                boolean[] inclusives = rangeSet.getInclusive();

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (value instanceof Slice slice) {
                        value = slice.toStringUtf8();
                    }
                    if (inclusives[i]) {
                        include.add(value);
                    }
                    else {
                        exclude.add(value);
                    }
                }

                if (!include.isEmpty()) {
                    ParametrizedString includePs = ParametrizedString.join(include.stream().map(v -> new ParametrizedString("?", Arrays.asList(v))).collect(Collectors.toUnmodifiableList()), ", ", String.format("%s IN [", left), "]");
                    if (domain.isNullableDiscreteSet()) {
                        includePs = ParametrizedString.join(
                                Arrays.asList(includePs, ParametrizedString.from(String.format("%s IS NULL", left))),
                                ") OR (", "(", ")");
                    }
                    ranges.add(includePs);
                }
                if (!exclude.isEmpty()) {
                    ranges.add(ParametrizedString.join(include.stream().map(v -> new ParametrizedString("?", Arrays.asList(v))).collect(Collectors.toUnmodifiableList()), ", ", String.format("%s NOT IN [", left), "]"));
                }
                return ParametrizedString.join(ranges, ") AND (", "(", ")");
            }
            else {
                rangeSet.getRanges().getOrderedRanges()
                        .stream().filter(range -> !range.isAll())
                        .forEach(range -> {
                            List<ParametrizedString> converted = new ArrayList<>();
                            if (!range.isLowUnbounded()) {
                                String op = "%s > ?";
                                if (range.isLowInclusive()) {
                                    op = "%s >= ?";
                                }
                                converted.add(ParametrizedString.from(String.format(op, left), Arrays.asList(TrinoToCbType.serialize(range.getType(), range.getLowValue()))));
                            }
                            if (!range.isHighUnbounded()) {
                                String op = "%s < ?";
                                if (range.isHighInclusive()) {
                                    op = "%s <= ?";
                                }
                                converted.add(ParametrizedString.from(String.format(op, left), Arrays.asList(TrinoToCbType.serialize(range.getType(), range.getHighValue()))));
                            }

                            ranges.add(ParametrizedString.join(converted, ") AND (", "(", ")"));
                        });
                if (domain.isNullAllowed()) {
                    ranges.add(ParametrizedString.from(String.format("%s IS NULL", left)));
                    return ParametrizedString.join(ranges, ") OR (", "(", ")");
                } else {
                    ParametrizedString nonNull = ParametrizedString.from(String.format("%s IS NOT NULL", left));
                    if (ranges.isEmpty()) {
                        return nonNull;
                    }

                    return ParametrizedString.join(List.of(
                            nonNull,
                            ParametrizedString.join(ranges, ") OR (", "(", ")")
                    ), " AND ");
                }
            }
        }
        else {
            throw new RuntimeException("Unsupported domain type: " + domain.getClass().getName());
        }
    }

    public boolean containsConstraint(Constraint constraint)
    {
        return whereClauses.contains(TrinoExpressionToCb.convert(constraint.getExpression(), constraint.getAssignments()));
    }

    @Override
    @NotNull
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(name);
        if (subQuery().isPresent()) {
            builder.append(String.format(" subQuery=[%s]",  subQuery()));
        }
        if (!whereClauses.isEmpty()) {
            builder.append(" filterPredicate=")
                    .append(ParametrizedString.join(whereClauses, ") AND (", "(", ")").text());
        }
        if (constraint.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!constraint.isAll()) {
            builder.append(" constraint on ");
            builder.append(constraint.getDomains().orElseThrow().keySet().stream()
                    .map(columnHandle -> ((CouchbaseColumnHandle) columnHandle).name())
                    .collect(joining(", ", "[", "]")));
        }
        if (!selectNames.isEmpty()) {
            builder.append(" columns=").append('[')
                    .append(String.join(", ", selectNames))
                    .append(']');
        }
        if (!orderClauses.isEmpty()) {
            builder.append(" sortOrder=")
                    .append(orderClauses.keySet().stream()
                            .map(s -> {
                                int space = s.indexOf(' ');
                                if (space > -1) {
                                    return String.format("%s:%s", s.substring(0, space), s.substring(space));
                                }
                                return String.format("%s:", s);
                            })
                            .collect(joining(", ", "[", "]")));
        }
        if (topNCount.get() > -1) {
            builder.append(" limit=").append(topNCount.get());
        }
        return builder.toString();
    }

    public void addPredicate(TupleDomain<ColumnHandle> predicate)
    {
        whereClauses.add(compilePredicate(predicate));
    }

    public ParametrizedString compilePredicate(TupleDomain<ColumnHandle> predicate)
    {
        List<ParametrizedString> clauses = new ArrayList<>();
        if (predicate.getDomains().isPresent() && !predicate.isAll()) {
            Map<ColumnHandle, Domain> domains = predicate.getDomains().get();
            if (!domains.isEmpty()) {
                domains.forEach((column, domain) -> {
                    if (column instanceof CouchbaseColumnHandle cbcolumn) {
                        clauses.add(compileDomain(cbcolumn.name(), domain));
                    }
                    else {
                        throw new IllegalArgumentException("Invalid column type: " + column.getClass().getName());
                    }
                });
            }
        }

        if (clauses.isEmpty()) {
            return ParametrizedString.from("TRUE");
        }
        if (clauses.size() == 1) {
            return clauses.get(0);
        }
        return ParametrizedString.join(clauses, ") AND (", "(", ")");
    }

    public void addColumns(Collection<CouchbaseColumnHandle> columns)
    {
        columns.forEach(this::addColumn);
    }

    public void addColumn(CouchbaseColumnHandle column)
    {
        if (!coversColumn(column)) {
            NamedParametrizedString nps = new NamedParametrizedString(column.name(), ParametrizedString.from(String.format("`%s`", column.name())));
            selectClauses.add(nps);
            selectTypes.add(column.type());
            selectNames.add(column.name());
            subQuery.ifPresent(sq -> {
                if (!sq.coversColumn(column)) {
                    sq.addColumn(column);
                }
            });
        }
    }

    public CouchbaseTableHandle wrap()
    {
        return new CouchbaseTableHandle(schema(), name(), Optional.of(this), new ArrayList<>(), new ArrayList<>(),
                new ArrayList<>(), new ArrayList<>(), TupleDomain.all(), new LinkedHashMap<>(), new HashSet<>(),
                new AtomicBoolean(false), new AtomicLong(-1));
    }

    public boolean containsProjections(List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return projections.stream()
                .map(projection -> compileProjection(projection, assignments))
                .allMatch(projection -> selectClauses.stream()
                        .anyMatch(clause ->
                                (projection.name() != null && Objects.equals(clause.name(), projection.name())) ||
                                (projection.name() == null && Objects.equals(clause.value(), projection.value()))));
    }

    public CouchbaseTableHandle withConstraint(TupleDomain<ColumnHandle> newDomain)
    {
        whereClauses.add(compilePredicate(newDomain));
        return new CouchbaseTableHandle(schema(), name(), subQuery, selectClauses, selectTypes, selectNames, whereClauses,
                newDomain, orderClauses, groupings, new AtomicBoolean(false), topNCount);
    }

    public boolean coversColumn(CouchbaseColumnHandle column)
    {
        return hasVariable(column.name());
    }

    public boolean containsAllAggregations(List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments) {
        return aggregates.stream().allMatch(agg -> containsAggregation(agg, assignments));
    }

    public boolean containsAggregation(AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments) {
        return findAggregation(aggregateFunction, assignments).isPresent();
    }

    public Optional<NamedParametrizedString> findAggregation(AggregateFunction aggregateFunction,
                                                    Map<String, ColumnHandle> assignments)
    {
        ParametrizedString converted = TrinoExpressionToCb.convert(aggregateFunction, assignments);
        return selectClauses.stream().filter(nps -> nps.value().equals(converted)).findFirst();
    }

    public NamedParametrizedString addAggregateFunction(AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        NamedParametrizedString result = new NamedParametrizedString(
                generateColumnName(),
                TrinoExpressionToCb.convert(aggregate, assignments));
        selectClauses.add(result);
        selectNames.add(result.name());
        selectTypes.add(aggregate.getOutputType());
        isAggregated.set(true);
        return result;
    }

    private String generateColumnName()
    {
        return String.format("syn_column_%d", selectClauses.size());
    }

    public void clearSelectElements()
    {
        selectClauses.clear();
        selectNames.clear();
        selectTypes.clear();
    }

    public boolean containsAllGroupings(Collection<? extends Collection<ColumnHandle>> groupingSets)
    {
        return groupingSets.stream()
                .flatMap(group -> group.stream().map(CouchbaseColumnHandle.class::cast))
                .allMatch(this.groupings::contains);
    }

    public void addGroupings(Collection<? extends Collection<ColumnHandle>> groupingSets)
    {
        groupingSets.stream()
                .flatMap(group -> group.stream().map(CouchbaseColumnHandle.class::cast))
                .forEach(chandle -> {
                    groupings.add(chandle);
                    if (!this.hasSortItemOn(chandle.name())) {
                        addSortItems(List.of(new SortItem(chandle.name(), SortOrder.ASC_NULLS_LAST)), Map.of(chandle.name(), chandle));
                    }
                });
    }

    public boolean hasSortItemOn(String columnName)
    {
        return this.orderClauses.values().stream()
                .filter(Objects::nonNull)
                .anyMatch(target -> target.name().equals(columnName));
    }
}
