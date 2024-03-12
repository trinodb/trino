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
import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

public class ExpressionAndValuePointers
{
    public static final ExpressionAndValuePointers TRUE = new ExpressionAndValuePointers(TRUE_LITERAL, ImmutableList.of());

    private final Expression expression;
    private final List<Assignment> assignments;

    @JsonCreator
    public ExpressionAndValuePointers(Expression expression, List<Assignment> assignments)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.assignments = ImmutableList.copyOf(assignments);
    }

    @JsonProperty
    public Expression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    public List<Symbol> getInputSymbols()
    {
        Set<Symbol> localInputs = assignments.stream()
                .filter(assignment -> assignment.valuePointer() instanceof ClassifierValuePointer || assignment.valuePointer() instanceof MatchNumberValuePointer)
                .map(Assignment::symbol)
                .collect(toImmutableSet());

        ImmutableList.Builder<Symbol> inputSymbols = ImmutableList.builder();
        for (Assignment assignment : assignments) {
            switch (assignment.valuePointer()) {
                case ScalarValuePointer pointer -> {
                    Symbol symbol = pointer.getInputSymbol();
                    if (!localInputs.contains(symbol)) {
                        inputSymbols.add(symbol);
                    }
                }
                case AggregationValuePointer pointer -> {
                    inputSymbols.addAll(pointer.getInputSymbols().stream()
                            .filter(symbol -> !localInputs.contains(symbol))
                            .collect(Collectors.toList()));
                }
                default -> {}
            }
        }

        return inputSymbols.build();
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
        ExpressionAndValuePointers o = (ExpressionAndValuePointers) obj;
        return Objects.equals(expression, o.expression) &&
                Objects.equals(assignments, o.assignments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, assignments);
    }

    public record Assignment(Symbol symbol, ValuePointer valuePointer) {}
}
