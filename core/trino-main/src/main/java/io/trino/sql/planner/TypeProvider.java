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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class TypeProvider
{
    private final Collection<Symbol> symbols;

    public static TypeProvider viewOf(Map<Symbol, Type> types)
    {
        return new TypeProvider(types.keySet());
    }

    public static TypeProvider copyOf(Map<Symbol, Type> types)
    {
        return new TypeProvider(ImmutableSet.copyOf(types.keySet()));
    }

    public static TypeProvider empty()
    {
        return new TypeProvider(ImmutableList.of());
    }

    private TypeProvider(Collection<Symbol> symbols)
    {
        this.symbols = symbols;
    }

    public static TypeProvider of(Collection<Symbol> symbols)
    {
        return new TypeProvider(symbols);
    }

    public Type get(Symbol symbol)
    {
        return symbol.getType();
    }

    public Map<Symbol, Type> allTypes()
    {
        return symbols.stream()
                .collect(toMap(s -> s, Symbol::getType));
    }
}
