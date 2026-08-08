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

import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolMapper;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolReallocator;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSymbolMapper
{
    @Test
    public void testReallocateSymbolsInLetKeepsBinderAndBodyConsistent()
    {
        Symbol input = new Symbol(VARCHAR, "input");
        Symbol bound = new Symbol(VARCHAR, "bound");
        Map<Symbol, Symbol> mapping = new HashMap<>();
        // Seed the allocator with the symbols being mapped so that reallocation actually renames them,
        // which is what copying a plan into another branch of the plan does.
        SymbolMapper mapper = symbolReallocator(mapping, new SymbolAllocator(List.of(input, bound)));

        Expression mapped = mapper.map(new Let(bound, input.toSymbolReference(), bound.toSymbolReference()));

        assertThat(mapped).isInstanceOf(Let.class);
        Let let = (Let) mapped;

        // The binder has to be reallocated together with the references to it, otherwise those
        // references are left dangling and the expression depends on a symbol nothing produces.
        assertThat(let.name()).isNotEqualTo(bound);
        assertThat(let.body()).isEqualTo(let.name().toSymbolReference());

        // Only the free symbol is a dependency; the bound one must not leak out.
        assertThat(SymbolsExtractor.extractUnique(let)).containsExactly(mapper.map(input));
    }

    @Test
    public void testReallocateSymbolsInNestedLet()
    {
        Symbol input = new Symbol(VARCHAR, "input");
        Symbol outer = new Symbol(VARCHAR, "outer");
        Symbol inner = new Symbol(VARCHAR, "inner");
        Map<Symbol, Symbol> mapping = new HashMap<>();
        SymbolMapper mapper = symbolReallocator(mapping, new SymbolAllocator(List.of(input, outer, inner)));

        Let let = (Let) mapper.map(new Let(
                outer,
                input.toSymbolReference(),
                new Let(inner, outer.toSymbolReference(), inner.toSymbolReference())));

        Let nested = (Let) let.body();
        assertThat(let.name()).isNotEqualTo(outer);
        assertThat(nested.name()).isNotEqualTo(inner);
        assertThat(nested.value()).isEqualTo(let.name().toSymbolReference());
        assertThat(nested.body()).isEqualTo(nested.name().toSymbolReference());
        assertThat(SymbolsExtractor.extractUnique(let)).containsExactly(mapper.map(input));
    }

    @Test
    public void testCanonicalizeLeavesLetBinderAloneWhenNotMapped()
    {
        Symbol from = new Symbol(VARCHAR, "from");
        Symbol to = new Symbol(VARCHAR, "to");
        Symbol bound = new Symbol(VARCHAR, "bound");
        SymbolMapper mapper = symbolMapper(Map.of(from, to));

        Let let = (Let) mapper.map(new Let(bound, from.toSymbolReference(), bound.toSymbolReference()));

        // Let-bound names are local to the expression, so alias canonicalization must not rename them.
        assertThat(let.name()).isEqualTo(bound);
        assertThat(let.value()).isEqualTo(to.toSymbolReference());
        assertThat(let.body()).isEqualTo(bound.toSymbolReference());
    }
}
