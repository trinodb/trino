package io.trino.plugin.couchbase;


import com.google.gson.internal.bind.DefaultDateTypeAdapter;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateType;
import jakarta.validation.constraints.NotNull;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public record CouchbaseTableHandle(
        String schema,
        String name,
        List<CouchbaseTableHandle> subQueries,
        Map<String, String> selectClauses,
        List<String> whereClauses,
        List<String> orderClauses,
        AtomicLong topNCount
        )
        implements ConnectorTableHandle
{

    public String path() {
        return String.format("`%s`.`%s`", schema, name);
    }

    public static CouchbaseTableHandle fromSchemaAndName(String schema, String name) {
        return new CouchbaseTableHandle(
                schema,
                name,
                new ArrayList<>(),
                new HashMap<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new AtomicLong(-1L)
        );
    }

    public void addSortItems(List<SortItem> sortItems) {
        sortItems.forEach(sortItem ->
                orderClauses.add(transformSortItem(sortItem)));
    }

    protected String transformSortItem(SortItem sortItem) {
        return String.format("%s %s", sortItem.getName(), sortItem.getSortOrder().toString());
    }

    public boolean compareSortItems(List<SortItem> sortItems) {
        if (this.orderClauses.size() != sortItems.size()) {
            return false;
        }
        return sortItems.stream()
                .map(this::transformSortItem)
                .allMatch(orderClauses::contains);
    }

    public boolean addAssignments(Map<String, ?> assignments, List<ConnectorExpression> projections) {
        return assignments.entrySet().stream()
            .filter(entry -> {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (selectClauses.containsKey(key)) {
                    return false;
                }
                // todo: type function calls
                CouchbaseColumnHandle column;
                if (value instanceof ColumnHandle) {
                    column = (CouchbaseColumnHandle) value;
                } else if (value instanceof Assignment) {
                    column = (CouchbaseColumnHandle) ((Assignment) value).getColumn();
                } else {
                    throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
                }

                ConnectorExpression projection = projections.stream()
                        .filter(Variable.class::isInstance)
                        .filter(prj -> ((Variable) prj).getName().equals(key))
                        .findFirst().orElse(null);

                if (projection == null) {
                    return false;
                }

                selectClauses.put(key, String.format("`%s`.`%s` `%s`", name, column.name(), key));
                return true;
            }).count() > 0;
    }

    public void setTopNCount(long topNCount) {
        this.topNCount.set(topNCount);
    }

    public void addProjections(List<ConnectorExpression> projections) {
        projections.forEach(projection -> {
            Map.Entry<String, String> compiled = compileProjection(projection);
            selectClauses.put(compiled.getKey(), compiled.getValue());
        });
    }

    private Map.Entry<String, String> compileProjection(ConnectorExpression projection) {
        if (projection instanceof Variable variable) {
            // todo: type function calls
            return Map.entry(variable.getName(), String.format("`%s`.`%s`", name, variable.getName()));
        } else {
            throw new UnsupportedOperationException("Unsupported projection type: " + projection.getClass().getName());
        }
    }

    public String toSql() {
        List<String> fromClause = new ArrayList<>();
        if (!subQueries.isEmpty()) {
            for (int i = 0; i < subQueries.size(); i++) {
                CouchbaseTableHandle subQuery = subQueries.get(i);
                if (subQuery == this || (subQuery.isEmpty() && subQuery.schema().equals(schema) && subQuery.name().equals(name))) {
                    continue;
                }

                String subqueryId = String.format("subquery%d", i);
                fromClause.add(String.format("(%s) `%s`", subQuery.toSql(), subqueryId));
                selectClauses.put(subqueryId, String.format("`%s`.*", subqueryId));
            }
        } else {
            fromClause.add(String.format("`%s`", name));
        }

        String query = String.format(
                "SELECT %s FROM %s WHERE %s ORDER BY %s",
                selectClauses.isEmpty() ? String.format("`%s`.*", name) : String.join(", ", selectClauses.values()),
                String.join(", ", fromClause),
                whereClauses.isEmpty() ? "TRUE" : String.join(" AND ", whereClauses),
                orderClauses.isEmpty() ? "META().id" : String.format("%s, META().id", String.join(", ", orderClauses))
        );

        if (topNCount.get() > -1) {
            query = String.format("%s LIMIT %d", query, topNCount.get());
        }

        return query;
    }

    public boolean isEmpty() {
        return topNCount.get() == -1 && selectClauses.isEmpty() && whereClauses.isEmpty() && orderClauses.isEmpty();
    }

    public void addConstraint(Constraint constraint) {
        whereClauses.add(compileConstraint(constraint));
    }

    protected String compileConstraint(Constraint constraint) {
        addAssignments(constraint.getAssignments(), Arrays.asList(constraint.getExpression()));
        TupleDomain filter = constraint.getSummary();
        if (filter.getDomains().isPresent()) {
            Map<CouchbaseColumnHandle, Domain> domains = (Map<CouchbaseColumnHandle, Domain>) filter.getDomains().get();
            List<String> clauses = new ArrayList<>();
            domains.forEach((column, domain) -> {
                clauses.add(String.format("%s %s", column.name(), compileDomain(domain)));
            });
            return String.format("(%s)", String.join(") AND (", clauses));
        }
        return "false";
    }

    private String compileDomain(Domain domain) {
        if (domain.isSingleValue()) {
            if (domain.getType() == DateType.DATE) {
                return String.format("= %d", domain.getSingleValue());
            } else {
                throw new RuntimeException("Unsupported domain value type: " + domain.getType());
            }
        }
        throw new RuntimeException("Unsupported domain type: " + domain.getClass().getName());
    }

    public boolean compareConstraint(Constraint constraint) {
        return false;
    }

    @Override
    @NotNull
    public String toString() {
        return String.format("CouchbaseTableHandle(%s.%s)", schema, name);
    }

    public boolean containsProjections(List<ConnectorExpression> expected) {
        return expected.stream()
                .map(this::compileProjection)
                .allMatch(compiled -> selectClauses.containsKey(compiled.getKey()) && selectClauses.get(compiled.getKey()).equals(compiled.getValue()));
    }
}
