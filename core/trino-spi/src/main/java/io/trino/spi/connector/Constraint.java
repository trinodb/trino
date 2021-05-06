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
import java.util.function.Predicate;

import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Objects.requireNonNull;

public class Constraint
{
    private final TupleDomain<ColumnHandle> summary;
    private final ConnectorExpression expression;
    private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;
    private final Optional<Set<ColumnHandle>> predicateColumns;

    public static Constraint alwaysTrue()
    {
        return new Constraint(TupleDomain.all());
    }

    public static Constraint alwaysFalse()
    {
        return new Constraint(TupleDomain.none(), bindings -> false, Set.of());
    }

    public Constraint(TupleDomain<ColumnHandle> summary)
    {
        this(summary, TRUE, Optional.empty(), Optional.empty());
    }

    public Constraint(TupleDomain<ColumnHandle> summary, Predicate<Map<ColumnHandle, NullableValue>> predicate, Set<ColumnHandle> predicateColumns)
    {
        this(summary, TRUE, Optional.of(predicate), Optional.of(predicateColumns));
    }

    public Constraint(TupleDomain<ColumnHandle> summary, ConnectorExpression expression)
    {
        this(summary, expression, Optional.empty(), Optional.empty());
    }

    public Constraint(
            TupleDomain<ColumnHandle> summary,
            ConnectorExpression expression,
            Predicate<Map<ColumnHandle, NullableValue>> predicate,
            Set<ColumnHandle> predicateColumns)
    {
        this(summary, expression, Optional.of(predicate), Optional.of(predicateColumns));
    }

    private Constraint(
            TupleDomain<ColumnHandle> summary,
            ConnectorExpression expression,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate,
            Optional<Set<ColumnHandle>> predicateColumns)
    {
        this.summary = requireNonNull(summary, "summary is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");

        if (predicateColumns.isPresent() && predicate.isEmpty()) {
            throw new IllegalArgumentException("predicateColumns cannot be present when predicate is not present");
        }
        if (predicateColumns.isEmpty() && predicate.isPresent()) {
            throw new IllegalArgumentException("predicate cannot be present without predicateColumns");
        }
    }

    public TupleDomain<ColumnHandle> getSummary()
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
     * A predicate that can be used to filter data. If present, it is equivalent to, or stricter than, {@link #getSummary()} and different from, and should be AND-ed with, {@link #getExpression()}.
     * <p>
     * For Constraint provided in {@link ConnectorMetadata#applyFilter(ConnectorSession, ConnectorTableHandle, Constraint)},
     * the predicate cannot be held on to after the call returns.
     *
     * @see #getPredicateColumns()
     */
    public Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate()
    {
        return predicate;
    }

    /**
     * Set of columns the {@link #predicate()} result depends on. It's present if and only if {@link #predicate()} is present.
     */
    public Optional<Set<ColumnHandle>> getPredicateColumns()
    {
        return predicateColumns;
    }
}
