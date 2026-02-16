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
package io.trino.sql.tree;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Resolver
{
    private final String catalog;
    private final BiFunction<String, Boolean, String> canonicalizer;
    private final BiFunction<String, Integer, String> comparator;
    private final Predicate<String> predicator;

    public Resolver(String catalog, BiFunction<String, Boolean, String> canonicalizer, BiFunction<String, Integer, String> comparator, Predicate<String> predicator)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.canonicalizer = requireNonNull(canonicalizer, "canonicalizer is null");
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.predicator = requireNonNull(predicator, "predicator is null");
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String canonicalize(Identifier identifier)
    {
        return canonicalize(identifier.getValue(), identifier.isDelimited());
    }

    public String canonicalize(String value, boolean delimited)
    {
        return canonicalizer.apply(value, delimited);
    }

    public String compare(String value, Integer type)
    {
        return comparator.apply(value, type);
    }

    public String compareSchema(String value)
    {
        return comparator.apply(value, Identifier.SCHEMA);
    }

    public String compareTable(String value)
    {
        return comparator.apply(value, Identifier.TABLE);
    }

    public String compareColumn(String value)
    {
        return comparator.apply(value, Identifier.COLUMN);
    }

    public boolean predicate(String value)
    {
        return predicator.test(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return catalog.equals(((Resolver) o).catalog);
    }

    @Override
    public int hashCode()
    {
        return catalog.hashCode();
    }
}
