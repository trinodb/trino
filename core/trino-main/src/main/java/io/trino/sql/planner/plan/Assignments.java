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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public record Assignments(Map<Symbol, Expression> assignments)
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderWithExpectedSize(int expectedSize)
    {
        return new Builder(expectedSize);
    }

    public static Assignments identity(Symbol... symbols)
    {
        return identity(asList(symbols));
    }

    public static Assignments identity(Collection<Symbol> symbols)
    {
        return Assignments.builderWithExpectedSize(symbols.size())
                .putIdentities(symbols)
                .build();
    }

    public static Assignments copyOf(Map<Symbol, Expression> assignments)
    {
        return Assignments.builderWithExpectedSize(assignments.size())
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return new Assignments(ImmutableMap.of());
    }

    public static Assignments of(Symbol symbol, Expression expression)
    {
        return Assignments.builderWithExpectedSize(1)
                .put(symbol, expression)
                .build();
    }

    public static Assignments of(Symbol symbol1, Expression expression1, Symbol symbol2, Expression expression2)
    {
        return Assignments.builderWithExpectedSize(2)
                .put(symbol1, expression1)
                .put(symbol2, expression2)
                .build();
    }

    public static Assignments of(Collection<? extends Expression> expressions, SymbolAllocator symbolAllocator)
    {
        Builder assignments = Assignments.builderWithExpectedSize(expressions.size());

        for (Expression expression : expressions) {
            assignments.put(symbolAllocator.newSymbol(expression), expression);
        }

        return assignments.build();
    }

    public Assignments
    {
        assignments = ImmutableMap.copyOf(assignments);
    }

    public List<Symbol> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    public Map<Symbol, Expression> getMap()
    {
        return assignments;
    }

    public Assignments rewrite(Function<Expression, Expression> rewrite)
    {
        ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builderWithExpectedSize(assignments.size());
        for (Entry<Symbol, Expression> entry : assignments.entrySet()) {
            builder.put(entry.getKey(), rewrite.apply(entry.getValue()));
        }
        return new Assignments(builder.buildOrThrow());
    }

    public Assignments filter(Collection<Symbol> symbols)
    {
        return filter(symbols::contains);
    }

    public Assignments filter(Predicate<Symbol> predicate)
    {
        ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
        for (Entry<Symbol, Expression> entry : assignments.entrySet()) {
            if (predicate.test(entry.getKey())) {
                builder.put(entry);
            }
        }
        return new Assignments(builder.buildOrThrow());
    }

    public boolean isIdentity(Symbol output)
    {
        Expression expression = assignments.get(output);

        return expression instanceof Reference reference && reference.name().equals(output.name());
    }

    public boolean isIdentity()
    {
        for (Entry<Symbol, Expression> entry : assignments.entrySet()) {
            Expression expression = entry.getValue();
            Symbol symbol = entry.getKey();
            if (!(expression instanceof Reference reference && reference.name().equals(symbol.name()))) {
                return false;
            }
        }
        return true;
    }

    public Collection<Expression> getExpressions()
    {
        return assignments.values();
    }

    public Set<Symbol> getSymbols()
    {
        return assignments.keySet();
    }

    public Set<Entry<Symbol, Expression>> entrySet()
    {
        return assignments.entrySet();
    }

    public Expression get(Symbol symbol)
    {
        return assignments.get(symbol);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void forEach(BiConsumer<Symbol, Expression> consumer)
    {
        assignments.forEach(consumer);
    }

    public record Assignment(Symbol output, Expression expression)
    {
        public Assignment
        {
            requireNonNull(output, "output is null");
            requireNonNull(expression, "expression is null");
        }
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<Symbol, Expression> assignments;

        private Builder()
        {
            assignments = ImmutableMap.builder();
        }

        private Builder(int expectedSize)
        {
            assignments = ImmutableMap.builderWithExpectedSize(expectedSize);
        }

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAll(Map<Symbol, ? extends Expression> assignments)
        {
            for (Entry<Symbol, ? extends Expression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(Symbol symbol, Expression expression)
        {
            checkArgument(symbol.type().equals(expression.type()), "Types don't match: %s vs %s, for %s and %s", symbol.type(), expression.type(), symbol, expression);
            assignments.put(symbol, expression);
            return this;
        }

        public Builder put(Entry<Symbol, Expression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Builder putIdentities(Iterable<Symbol> symbols)
        {
            for (Symbol symbol : symbols) {
                putIdentity(symbol);
            }
            return this;
        }

        public Builder putIdentity(Symbol symbol)
        {
            put(symbol, symbol.toSymbolReference());
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments.buildOrThrow());
        }

        public Builder add(Assignment assignment)
        {
            put(assignment.output(), assignment.expression());
            return this;
        }
    }
}
