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
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Resolver
{
    public static final Function<Identifier, String> DEFAULT_CANONICALIZER = Identifier::getValue;
    public static final CanonicalizerKind getCanonicalizerKind(Function<Identifier, String> canonicalizer)
    {
        return getCanonicalizerKind(canonicalizer.apply(new Identifier("Xy", false)));
    }

    private final String catalog;
    private final BiFunction<String, Boolean, String> canonicalizer;
    private final BiFunction<String, IdentifierKind, String> comparator;
    private final Predicate<String> predicator;
    private final CanonicalizerKind canonicalizerKind;

    public enum CanonicalizerKind
    {
        IDENTITY, LOWER_CASE, UPPER_CASE
    }

    public Resolver(String catalog, BiFunction<String, Boolean, String> canonicalizer, BiFunction<String, IdentifierKind, String> comparator, Predicate<String> predicator)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.canonicalizer = requireNonNull(canonicalizer, "canonicalizer is null");
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.predicator = requireNonNull(predicator, "predicator is null");
        this.canonicalizerKind = getCanonicalizerKind(canonicalizer);
    }

    private static CanonicalizerKind getCanonicalizerKind(BiFunction<String, Boolean, String> canonicalizer)
    {
        return getCanonicalizerKind(canonicalizer.apply("Xy", false));
    }

    private static CanonicalizerKind getCanonicalizerKind(String value)
    {
        return switch (value){
            case "Xy" -> CanonicalizerKind.IDENTITY;
            case "xy" -> CanonicalizerKind.LOWER_CASE;
            case "XY" -> CanonicalizerKind.UPPER_CASE;
            default -> throw new IllegalStateException("Unexpected value: " + value);
        };
    }

    public CanonicalizerKind getCanonicalizerKind()
    {
        return canonicalizerKind;
    }

    public Function<Identifier, String> getCanonicalizer()
    {
        return identifier -> canonicalizer.apply(identifier.getValue(), identifier.isDelimited());
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String canonicalize(Identifier identifier)
    {
        return canonicalizer.apply(identifier.getValue(), identifier.isDelimited());
    }

    public String compare(String value, IdentifierKind kind)
    {
        return comparator.apply(value, kind);
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
