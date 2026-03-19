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
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.couchbase.translations.TrinoExpressionToCb;
import io.trino.plugin.couchbase.translations.TrinoToCbType;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SortItem;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public record CouchbaseTableHandle(String schema, String name, Optional<CouchbaseTableHandle> subQuery,
                                   List<NamedParametrizedString> selectClauses, List<Type> selectTypes,
                                   List<String> selectNames, List<ParametrizedString> whereClauses,
                                   TupleDomain<ColumnHandle> constraint, List<String> orderClauses,
                                   Set<List<ColumnHandle>> groupings,
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
                null,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                TupleDomain.all(),
                new ArrayList<>(),
                new HashSet<>(),
                new AtomicLong(-1L));
    }

    public void addSortItems(List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        sortItems.forEach(sortItem -> orderClauses.add(transformSortItem(sortItem, assignments)));
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
        return sortItems.stream().map(si -> transformSortItem(si, assignments)).allMatch(orderClauses::contains);
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

    public String toSql()
    {
        List<String> fromClause = new ArrayList<>();
        if (subQuery.isPresent()) {
            CouchbaseTableHandle sq = subQuery.get();
            if (this.topNCount.get() < 0 && this.whereClauses().isEmpty() && this.orderClauses.isEmpty() && sq.selectClauses().containsAll(this.selectClauses)) {
                return sq.toSql();
            }
            if (sq != this && !(sq.isEmpty() && sq.schema().equals(schema) && sq.name().equals(name))) {
                fromClause.add(String.format("(%s) `%s`", sq.toSql(), "data"));
//                    selectClauses.add(new NamedParametrizedString("data", ParametrizedString.from(String.format("`%s`.*", "data"))));
            }
        }
        if (fromClause.isEmpty()) {
            fromClause.add(String.format("`%s`", name));
        }

        String query = String.format("SELECT %s FROM %s WHERE %s ORDER BY %s",
                selectClauses.isEmpty() ? "{}" : selectClauses.stream().map(NamedParametrizedString::toString).collect(joining(", ")),
                String.join(", ", fromClause),
                whereClauses.isEmpty() ? "TRUE" : whereClauses.stream().map(ParametrizedString::toString).collect(joining(", ")),
                orderClauses.isEmpty() ? "META().id" : String.format("%s, META().id", String.join(", ", orderClauses)));

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
        return topNCount.get() == -1 && selectClauses.isEmpty() && whereClauses.isEmpty() && orderClauses.isEmpty();
    }

    public void addConstraint(Constraint constraint)
    {
        whereClauses.add(compileConstraint(constraint));
    }

    protected ParametrizedString compileConstraint(Constraint constraint)
    {
//        addAssignments(constraint.getAssignments(), Arrays.asList(constraint.getExpression()));
        TupleDomain<ColumnHandle> filter = constraint.getSummary();
        List<ParametrizedString> clauses = new ArrayList<>();
        clauses.add(compilePredicate(filter));

        if (!constraint.getExpression().equals(Constant.TRUE)) {
            clauses.add(TrinoExpressionToCb.convert(constraint.getExpression(), constraint.getAssignments()));
        }

        if (clauses.isEmpty()) {
            return ParametrizedString.from("TRUE");
        }
        else if (clauses.size() == 1) {
            return clauses.getFirst();
        }
        return ParametrizedString.join(clauses, ") AND (", "(", ")");
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
                    if (inclusives[i]) {
                        include.add(values.get(i));
                    }
                    else {
                        exclude.add(values.get(i));
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
                rangeSet.getRanges().getOrderedRanges().forEach(range -> {
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
                return ParametrizedString.join(ranges, ") OR (", "(", ")");
            }
        }
        else {
            throw new RuntimeException("Unsupported domain type: " + domain.getClass().getName());
        }
    }

    public boolean containsConstraint(Constraint constraint)
    {
        return whereClauses.contains(compileConstraint(constraint));
    }

    @Override
    @NotNull
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(name);
        if (!whereClauses.isEmpty()) {
            builder.append(" filter=")
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
                    .append(orderClauses.stream()
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
        NamedParametrizedString nps = new NamedParametrizedString(column.name(), ParametrizedString.from(String.format("`%s`", column.name())));
        if (!selectClauses.contains(nps)) {
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
        return new CouchbaseTableHandle(schema(), name(), Optional.of(this), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), TupleDomain.all(), new ArrayList<>(), new HashSet<>(), new AtomicLong(-1));
    }

    public boolean containsProjections(List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return projections.stream().map(projection -> compileProjection(projection, assignments)).allMatch(nps -> selectClauses.contains(nps));
    }

    public boolean coversAllColumns(Set<CouchbaseColumnHandle> projectedColumns)
    {
        return projectedColumns.stream().allMatch(this::coversColumn);
    }

    public void addColumns(Map<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> columnProjections, Map<String, ColumnHandle> assignments)
    {
        columnProjections.forEach((expression, columnRepresentation) -> {
            addProjection(expression, assignments);
        });
    }

    public CouchbaseTableHandle withConstraint(TupleDomain<ColumnHandle> newDomain)
    {
        ArrayList<ParametrizedString> where = new ArrayList<>();
        where.add(compilePredicate(newDomain));
        return new CouchbaseTableHandle(schema(), name(), subQuery, selectClauses, selectTypes, selectNames, where, newDomain, orderClauses, groupings, topNCount);
    }

    public boolean coversColumn(CouchbaseColumnHandle column)
    {
        return hasVariable(column.name());
    }

    public void addAggregateFunctions(List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments)
    {
        aggregates.forEach(a -> addAggregateFunction(a, assignments));
    }

    public void addAggregateFunction(AggregateFunction a, Map<String, ColumnHandle> assignments)
    {
        selectClauses.add(new NamedParametrizedString(null, ParametrizedString.join(
                TrinoExpressionToCb.convert(a.getArguments(), assignments), ", ", String.format("%s(", a.getFunctionName()), ")")));
        selectNames.add(null);
        selectTypes.add(a.getOutputType());
    }

    public void clearSelectElements()
    {
        selectClauses.clear();
        selectNames.clear();
        selectTypes.clear();
    }
}
