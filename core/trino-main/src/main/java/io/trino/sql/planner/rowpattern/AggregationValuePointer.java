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
package io.trino.sql.planner.rowpattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class AggregationValuePointer
        implements ValuePointer
{
    private final ResolvedFunction function;
    private final AggregatedSetDescriptor setDescriptor;
    private final List<Expression> arguments;
    private final Optional<Symbol> classifierSymbol;
    private final Optional<Symbol> matchNumberSymbol;

    @JsonCreator
    public AggregationValuePointer(ResolvedFunction function, AggregatedSetDescriptor setDescriptor, List<Expression> arguments, Optional<Symbol> classifierSymbol, Optional<Symbol> matchNumberSymbol)
    {
        this.function = requireNonNull(function, "function is null");
        this.setDescriptor = requireNonNull(setDescriptor, "setDescriptor is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.classifierSymbol = requireNonNull(classifierSymbol, "classifierSymbol is null");
        this.matchNumberSymbol = requireNonNull(matchNumberSymbol, "matchNumberSymbol is null");
    }

    @JsonProperty
    public ResolvedFunction getFunction()
    {
        return function;
    }

    @JsonProperty
    public AggregatedSetDescriptor getSetDescriptor()
    {
        return setDescriptor;
    }

    @JsonProperty
    public List<Expression> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public Optional<Symbol> getClassifierSymbol()
    {
        return classifierSymbol;
    }

    @JsonProperty
    public Optional<Symbol> getMatchNumberSymbol()
    {
        return matchNumberSymbol;
    }

    public List<Symbol> getInputSymbols()
    {
        return arguments.stream()
                .map(SymbolsExtractor::extractAll)
                .flatMap(Collection::stream)
                .filter(symbol -> (classifierSymbol.isEmpty() || !classifierSymbol.get().equals(symbol)) &&
                    (matchNumberSymbol.isEmpty() || !matchNumberSymbol.get().equals(symbol)))
                .collect(toImmutableList());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AggregationValuePointer o = (AggregationValuePointer) obj;
        return Objects.equals(function, o.function) &&
                Objects.equals(setDescriptor, o.setDescriptor) &&
                Objects.equals(arguments, o.arguments) &&
                Objects.equals(classifierSymbol, o.classifierSymbol) &&
                Objects.equals(matchNumberSymbol, o.matchNumberSymbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, setDescriptor, arguments, classifierSymbol, matchNumberSymbol);
    }
}
