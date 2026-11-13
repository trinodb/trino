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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Call;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.Assignments;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.operator.scalar.CollationKeyFunction.NAME;
import static io.trino.type.collation.Collations.hasNonDefaultCollation;

/// Replaces collated string symbols by their collation keys.
///
/// Equal collation keys identify strings equal under the collation, so grouping by the key forms
/// the same groups as grouping by the string. The unsigned byte order of a collation key is its
/// collation's, so ordering or partitioning by the key orders and partitions the rows as by the
/// string. A null string has a null key, so null ordering is preserved.
final class CollationKeys
{
    private final Metadata metadata;
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Map<Symbol, Symbol> keys = new LinkedHashMap<>();
    private final Assignments.Builder assignments = Assignments.builder();

    CollationKeys(Metadata metadata, Session session, SymbolAllocator symbolAllocator)
    {
        this.metadata = metadata;
        this.session = session;
        this.symbolAllocator = symbolAllocator;
    }

    static boolean isCollated(Symbol symbol)
    {
        return symbol.type() instanceof VarcharType && hasNonDefaultCollation(symbol.type());
    }

    static boolean hasCollated(OrderingScheme orderingScheme)
    {
        return orderingScheme.orderBy().stream().anyMatch(CollationKeys::isCollated);
    }

    static boolean hasCollated(List<Symbol> symbols)
    {
        return symbols.stream().anyMatch(CollationKeys::isCollated);
    }

    /// The key of a collated symbol, or the symbol itself.
    Symbol key(Symbol symbol)
    {
        if (!isCollated(symbol)) {
            return symbol;
        }
        return keys.computeIfAbsent(symbol, _ -> {
            ResolvedFunction collationKey = metadata.resolveBuiltinFunction(getCharVarcharCoercion(session), NAME, List.of(symbol.type()));
            Symbol key = symbolAllocator.newSymbol(symbol.name() + "_key", collationKey.signature().getReturnType());
            assignments.put(key, new Call(collationKey, ImmutableList.of(symbol.toSymbolReference())));
            return key;
        });
    }

    OrderingScheme keys(OrderingScheme orderingScheme)
    {
        ImmutableList.Builder<Symbol> orderBy = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
        for (Symbol symbol : orderingScheme.orderBy()) {
            Symbol key = key(symbol);
            orderBy.add(key);
            orderings.put(key, orderingScheme.ordering(symbol));
        }
        return new OrderingScheme(orderBy.build(), orderings.buildOrThrow());
    }

    Optional<OrderingScheme> keys(Optional<OrderingScheme> orderingScheme)
    {
        return orderingScheme.map(this::keys);
    }

    List<Symbol> keys(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(this::key)
                .collect(toImmutableList());
    }

    /// The assignments that compute the keys, together with identities for the given symbols.
    Assignments assignments(List<Symbol> inputs)
    {
        return Assignments.builder()
                .putIdentities(inputs)
                .putAll(assignments.build())
                .build();
    }
}
