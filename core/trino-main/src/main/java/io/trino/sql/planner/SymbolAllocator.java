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

import com.google.common.base.CharMatcher;
import com.google.common.primitives.Ints;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Field;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class SymbolAllocator
{
    public static final CharMatcher EXCLUDED_CHARACTERS = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('0', '9'))
            .negate()
            .precomputed();

    private final Map<String, Symbol> symbols;
    private int nextId;

    public SymbolAllocator()
    {
        symbols = new HashMap<>();
    }

    public SymbolAllocator(Collection<Symbol> initial)
    {
        symbols = new HashMap<>(initial.stream()
                .collect(toMap(Symbol::name, e -> e)));
    }

    public Symbol newSymbol(Symbol symbolHint)
    {
        return newSymbol(symbolHint.name(), symbolHint.type());
    }

    public Symbol newSymbol(String nameHint, Type type)
    {
        requireNonNull(nameHint, "nameHint is null");
        requireNonNull(type, "type is null");

        nameHint = EXCLUDED_CHARACTERS.trimAndCollapseFrom(nameHint.toLowerCase(ENGLISH), '_');

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        Symbol symbol = new Symbol(type, nameHint);
        while (symbols.putIfAbsent(symbol.name(), symbol) != null) {
            symbol = new Symbol(type, nameHint + "_" + nextId());
        }

        return symbol;
    }

    public Symbol newSymbol(Expression expression)
    {
        String nameHint = switch (expression) {
            case Call call -> call.function().name().getFunctionName();
            case Reference reference -> reference.name();
            default -> "expr";
        };

        return newSymbol(nameHint, expression.type());
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(nameHint, field.getType());
    }

    public Collection<Symbol> getSymbols()
    {
        return symbols.values();
    }

    private int nextId()
    {
        return nextId++;
    }
}
