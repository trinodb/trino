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
package io.trino.spi.connector;

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Objects.requireNonNull;

public class Constraint<T>
{
    private static final Constraint<?> ALWAYS_TRUE = new Constraint<>(TupleDomain.all());
    private static final Constraint<?> ALWAYS_FALSE = new Constraint<>(TupleDomain.none(), bindings -> false, Set.of());

    private final TupleDomain<T> summary;
    private final ConnectorExpression expression;
    private final Map<String, T> assignments;
    private final Optional<Predicate<Map<T, NullableValue>>> predicate;
    private final Optional<Set<T>> predicateColumns;

    @SuppressWarnings("unchecked")
    public static <T> Constraint<T> alwaysTrue()
    {
        return (Constraint<T>) ALWAYS_TRUE;
    }

    @SuppressWarnings("unchecked")
    public static <T> Constraint<T> alwaysFalse()
    {
        return (Constraint<T>) ALWAYS_FALSE;
    }

    public Constraint(TupleDomain<T> summary)
    {
        this(summary, TRUE, Map.of(), Optional.empty(), Optional.empty());
    }

    public Constraint(TupleDomain<T> summary, Predicate<Map<T, NullableValue>> predicate, Set<T> predicateColumns)
    {
        this(summary, TRUE, Map.of(), Optional.of(predicate), Optional.of(predicateColumns));
    }

    public Constraint(TupleDomain<T> summary, ConnectorExpression expression, Map<String, T> assignments)
    {
        this(summary, expression, assignments, Optional.empty(), Optional.empty());
    }

    public Constraint(
            TupleDomain<T> summary,
            ConnectorExpression expression,
            Map<String, T> assignments,
            Predicate<Map<T, NullableValue>> predicate,
            Set<T> predicateColumns)
    {
        this(summary, expression, assignments, Optional.of(predicate), Optional.of(predicateColumns));
    }

    private Constraint(
            TupleDomain<T> summary,
            ConnectorExpression expression,
            Map<String, T> assignments,
            Optional<Predicate<Map<T, NullableValue>>> predicate,
            Optional<Set<T>> predicateColumns)
    {
        this.summary = requireNonNull(summary, "summary is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.assignments = Map.copyOf(requireNonNull(assignments, "assignments is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.predicateColumns = predicateColumns.map(Set::copyOf);

        if (predicateColumns.isPresent() && predicate.isEmpty()) {
            throw new IllegalArgumentException("predicateColumns cannot be present when predicate is not present");
        }
        if (predicateColumns.isEmpty() && predicate.isPresent()) {
            throw new IllegalArgumentException("predicate cannot be present without predicateColumns");
        }
    }

    /**
     * @return a predicate which is equivalent to, or looser than {@link #predicate} (if present), and should be AND-ed with, {@link #getExpression}.
     */
    public TupleDomain<T> getSummary()
    {
        return summary;
    }

    /**
     * @return an expression predicate which is different from, and should be AND-ed with, {@link #getSummary} or {@link #predicate} (if present).
     */
    public ConnectorExpression getExpression()
    {
        return expression;
    }

    /**
     * @return mappings from variable names to table column handles
     * It is guaranteed that all the required mappings for {@link #getExpression} will be provided but not necessarily *all* the column handles of the table
     */
    public Map<String, T> getAssignments()
    {
        return assignments;
    }

    /**
     * A predicate that can be used to filter data. If present, it is equivalent to, or stricter than, {@link #getSummary()} and different from, and should be AND-ed with, {@link #getExpression()}.
     * <p>
     * For Constraint provided in {@link ConnectorMetadata#applyFilter(ConnectorSession, ConnectorTableHandle, Constraint)},
     * the predicate cannot be held on to after the call returns.
     *
     * @see #getPredicateColumns()
     */
    public Optional<Predicate<Map<T, NullableValue>>> predicate()
    {
        return predicate;
    }

    /**
     * Set of columns the {@link #predicate()} result depends on. It's present if and only if {@link #predicate()} is present.
     */
    public Optional<Set<T>> getPredicateColumns()
    {
        return predicateColumns;
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Constraint.class.getSimpleName() + "[", "]");
        stringJoiner.add("summary=" + summary);
        stringJoiner.add("expression=" + expression);
        predicate.ifPresent(predicate -> stringJoiner.add("predicate=" + predicate));
        predicateColumns.ifPresent(predicateColumns -> stringJoiner.add("predicateColumns=" + predicateColumns));
        return stringJoiner.toString();
    }
}
