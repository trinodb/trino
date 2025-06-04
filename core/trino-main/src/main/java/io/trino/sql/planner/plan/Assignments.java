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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class Assignments
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Assignments identity(Symbol... symbols)
    {
        return identity(asList(symbols));
    }

    public static Assignments identity(Iterable<Symbol> symbols)
    {
        return builder().putIdentities(symbols).build();
    }

    public static Assignments copyOf(Map<Symbol, Expression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(Symbol symbol, Expression expression)
    {
        return builder().put(symbol, expression).build();
    }

    public static Assignments of(Symbol symbol1, Expression expression1, Symbol symbol2, Expression expression2)
    {
        return builder().put(symbol1, expression1).put(symbol2, expression2).build();
    }

    public static Assignments of(Collection<? extends Expression> expressions, SymbolAllocator symbolAllocator)
    {
        Assignments.Builder assignments = Assignments.builder();

        for (Expression expression : expressions) {
            assignments.put(symbolAllocator.newSymbol(expression), expression);
        }

        return assignments.build();
    }

    private final Map<Symbol, Expression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<Symbol, Expression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Symbol> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<Symbol, Expression> getMap()
    {
        return assignments;
    }

    public Assignments rewrite(Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), rewrite.apply(entry.getValue())))
                .collect(toAssignments());
    }

    public Assignments filter(Collection<Symbol> symbols)
    {
        return filter(symbols::contains);
    }

    public Assignments filter(Predicate<Symbol> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.test(entry.getKey()))
                .collect(toAssignments());
    }

    public boolean isIdentity(Symbol output)
    {
        Expression expression = assignments.get(output);

        return expression instanceof Reference reference && reference.name().equals(output.name());
    }

    public boolean isIdentity()
    {
        for (Map.Entry<Symbol, Expression> entry : assignments.entrySet()) {
            Expression expression = entry.getValue();
            Symbol symbol = entry.getKey();
            if (!(expression instanceof Reference reference && reference.name().equals(symbol.name()))) {
                return false;
            }
        }
        return true;
    }

    private Collector<Entry<Symbol, Expression>, Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Assignments.Builder::build);
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignments that = (Assignments) o;

        return assignments.equals(that.assignments);
    }

    @Override
    public int hashCode()
    {
        return assignments.hashCode();
    }

    public static class Assignment
    {
        private final Symbol output;
        private final Expression expression;

        public Assignment(Symbol output, Expression expression)
        {
            this.output = requireNonNull(output, "output is null");
            this.expression = requireNonNull(expression, "expression is null");
        }

        public Symbol getOutput()
        {
            return output;
        }

        public Expression getExpression()
        {
            return expression;
        }
    }

    public static class Builder
    {
        private final Map<Symbol, Expression> assignments = new LinkedHashMap<>();

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

            Expression replaced = assignments.put(symbol, expression);
            if (replaced != null && !replaced.equals(expression)) {
                assignments.put(symbol, replaced); // restore previous value
                throw new IllegalStateException("Symbol %s already has assignment %s, while adding %s".formatted(symbol.name(), replaced, expression));
            }
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
            return new Assignments(assignments);
        }

        public Builder add(Assignment assignment)
        {
            put(assignment.getOutput(), assignment.getExpression());
            return this;
        }
    }
}
