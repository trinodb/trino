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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Disjoint sets of symbols that are known to hold identical values in every row of a plan node's output.
 * Symbols in the same class can be used interchangeably when reasoning about sorting and grouping.
 */
public final class SymbolEquivalences
{
    private static final SymbolEquivalences EMPTY = new SymbolEquivalences(ImmutableSet.of());

    private final Set<Set<Symbol>> classes;
    private final Map<Symbol, Set<Symbol>> classBySymbol;
    private final Map<Symbol, Symbol> representatives;

    private SymbolEquivalences(Set<Set<Symbol>> classes)
    {
        this.classes = ImmutableSet.copyOf(classes);
        ImmutableMap.Builder<Symbol, Set<Symbol>> builder = ImmutableMap.builder();
        for (Set<Symbol> symbols : classes) {
            for (Symbol symbol : symbols) {
                builder.put(symbol, symbols);
            }
        }
        this.classBySymbol = builder.buildOrThrow();
        ImmutableMap.Builder<Symbol, Symbol> representativesBuilder = ImmutableMap.builder();
        for (Set<Symbol> symbols : classes) {
            Symbol representative = symbols.stream()
                    .min(Comparator.comparing(Symbol::name))
                    .orElseThrow();
            symbols.forEach(symbol -> representativesBuilder.put(symbol, representative));
        }
        this.representatives = representativesBuilder.buildOrThrow();
    }

    public static SymbolEquivalences empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return classes.isEmpty();
    }

    public Set<Set<Symbol>> getClasses()
    {
        return classes;
    }

    /**
     * Deterministic representative of the class the symbol belongs to, or the symbol itself if it is in no class.
     */
    public Symbol representative(Symbol symbol)
    {
        return representatives.getOrDefault(symbol, symbol);
    }

    public Set<Symbol> equivalentsOf(Symbol symbol)
    {
        return classBySymbol.getOrDefault(symbol, ImmutableSet.of(symbol));
    }

    /**
     * Whether values of the two symbols are equal in the SQL sense and also indistinguishable for sorting and grouping.
     */
    public static boolean canEquate(Symbol left, Symbol right)
    {
        return !left.equals(right) && left.type().equals(right.type()) && hasConsistentEquality(left.type());
    }

    // SQL '=' treats e.g. 0.0 and -0.0 or equal instants in different zones as equal, but sorting and grouping may tell them apart
    private static boolean hasConsistentEquality(Type type)
    {
        String base = type.getBaseName();
        switch (base) {
            case StandardTypes.REAL, StandardTypes.DOUBLE, StandardTypes.TIMESTAMP_WITH_TIME_ZONE, StandardTypes.TIME_WITH_TIME_ZONE -> {
                return false;
            }
            default -> {
                return type.getTypeParameters().stream().allMatch(SymbolEquivalences::hasConsistentEquality);
            }
        }
    }

    public SymbolEquivalences with(Symbol left, Symbol right)
    {
        return with(List.of(List.of(left, right)));
    }

    /**
     * Adds the given groups of symbols as classes, merging with existing classes they overlap. Groups containing symbols
     * that cannot be equated with each other are ignored.
     */
    public SymbolEquivalences with(Collection<? extends Collection<Symbol>> groups)
    {
        List<Set<Symbol>> merged = new ArrayList<>();
        for (Set<Symbol> existing : classes) {
            merged.add(new LinkedHashSet<>(existing));
        }
        for (Collection<Symbol> group : groups) {
            if (group.size() < 2) {
                continue;
            }
            Symbol first = group.iterator().next();
            if (!group.stream().allMatch(symbol -> symbol.equals(first) || canEquate(first, symbol))) {
                continue;
            }
            Set<Symbol> combined = new LinkedHashSet<>(group);
            if (combined.size() < 2) {
                continue;
            }
            merged.removeIf(existing -> {
                if (existing.stream().anyMatch(combined::contains)) {
                    combined.addAll(existing);
                    return true;
                }
                return false;
            });
            merged.add(combined);
        }
        return new SymbolEquivalences(ImmutableSet.copyOf(merged));
    }

    public SymbolEquivalences merge(SymbolEquivalences other)
    {
        if (other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        return with(other.classes);
    }

    /**
     * Keeps only symbols that survive the translation, mapping each to its new name. Classes left with a single symbol are dropped.
     */
    public SymbolEquivalences translate(Function<Symbol, Optional<Symbol>> translator)
    {
        if (isEmpty()) {
            return this;
        }
        Map<Set<Symbol>, Set<Symbol>> translated = new HashMap<>();
        for (Set<Symbol> symbols : classes) {
            Set<Symbol> survivors = symbols.stream()
                    .map(translator)
                    .flatMap(Optional::stream)
                    .collect(toImmutableSet());
            if (survivors.size() > 1) {
                translated.put(symbols, survivors);
            }
        }
        return empty().with(new HashSet<>(translated.values()));
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SymbolEquivalences that && classes.equals(that.classes);
    }

    @Override
    public int hashCode()
    {
        return classes.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("classes", classes)
                .toString();
    }
}
