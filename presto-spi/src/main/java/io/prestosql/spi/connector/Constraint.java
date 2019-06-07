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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Constraint
{
    private final TupleDomain<ColumnHandle> summary;
    private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;

    public static Constraint alwaysTrue()
    {
        return new Constraint(TupleDomain.all(), Optional.empty());
    }

    public static Constraint alwaysFalse()
    {
        return new Constraint(TupleDomain.none(), Optional.of(bindings -> false));
    }

    public Constraint(TupleDomain<ColumnHandle> summary)
    {
        this(summary, Optional.empty());
    }

    public Constraint(TupleDomain<ColumnHandle> summary, Predicate<Map<ColumnHandle, NullableValue>> predicate)
    {
        this(summary, Optional.of(predicate));
    }

    public Constraint(TupleDomain<ColumnHandle> summary, Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        requireNonNull(summary, "summary is null");
        requireNonNull(predicate, "predicate is null");

        this.summary = summary;
        this.predicate = predicate;
    }

    public TupleDomain<ColumnHandle> getSummary()
    {
        return summary;
    }

    public Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate()
    {
        return predicate;
    }
}
