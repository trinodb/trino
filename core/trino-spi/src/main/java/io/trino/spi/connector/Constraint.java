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

import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Constraint
{
    private final TupleDomain<ColumnHandle> summary;
    private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;
    private final Optional<Set<ColumnHandle>> predicateColumns;

    public static Constraint alwaysTrue()
    {
        return new Constraint(TupleDomain.all(), Optional.empty(), Optional.empty());
    }

    public static Constraint alwaysFalse()
    {
        return new Constraint(TupleDomain.none(), Optional.of(bindings -> false), Optional.empty());
    }

    public Constraint(TupleDomain<ColumnHandle> summary)
    {
        this(summary, Optional.empty(), Optional.empty());
    }

    public Constraint(TupleDomain<ColumnHandle> summary, Predicate<Map<ColumnHandle, NullableValue>> predicate, Set<ColumnHandle> predicateColumns)
    {
        this(summary, Optional.of(predicate), Optional.of(predicateColumns));
    }

    public Constraint(TupleDomain<ColumnHandle> summary, Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate, Optional<Set<ColumnHandle>> predicateColumns)
    {
        this.summary = requireNonNull(summary, "summary is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");

        // TODO remove deprecated constructors and validate that predicate is present *iff* predicateColumns is present
        if (predicateColumns.isPresent() && predicate.isEmpty()) {
            throw new IllegalArgumentException("predicateColumns cannot be present when predicate is not present");
        }
    }

    public TupleDomain<ColumnHandle> getSummary()
    {
        return summary;
    }

    public Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate()
    {
        return predicate;
    }

    /**
     * @deprecated Use {@link #getPredicateColumns()} instead.
     */
    @Deprecated
    public Optional<Set<ColumnHandle>> getColumns()
    {
        return getPredicateColumns();
    }

    /**
     * Set of columns the {@link #predicate()} result depends on.
     */
    public Optional<Set<ColumnHandle>> getPredicateColumns()
    {
        return predicateColumns;
    }
}
