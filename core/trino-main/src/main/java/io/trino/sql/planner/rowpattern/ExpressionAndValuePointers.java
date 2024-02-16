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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

public class ExpressionAndValuePointers
{
    public static final ExpressionAndValuePointers TRUE = new ExpressionAndValuePointers(TRUE_LITERAL, ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of());

    private final Expression expression;
    private final List<Symbol> layout;
    private final List<ValuePointer> valuePointers;
    private final Set<Symbol> classifierSymbols;
    private final Set<Symbol> matchNumberSymbols;

    @JsonCreator
    public ExpressionAndValuePointers(Expression expression, List<Symbol> layout, List<ValuePointer> valuePointers, Set<Symbol> classifierSymbols, Set<Symbol> matchNumberSymbols)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.valuePointers = requireNonNull(valuePointers, "valuePointers is null");
        checkArgument(layout.size() == valuePointers.size(), "layout and valuePointers sizes don't match");
        this.classifierSymbols = requireNonNull(classifierSymbols, "classifierSymbols is null");
        this.matchNumberSymbols = requireNonNull(matchNumberSymbols, "matchNumberSymbols is null");
    }

    @JsonProperty
    public Expression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public List<Symbol> getLayout()
    {
        return layout;
    }

    @JsonProperty
    public List<ValuePointer> getValuePointers()
    {
        return valuePointers;
    }

    @JsonProperty
    public Set<Symbol> getClassifierSymbols()
    {
        return classifierSymbols;
    }

    @JsonProperty
    public Set<Symbol> getMatchNumberSymbols()
    {
        return matchNumberSymbols;
    }

    public List<Symbol> getInputSymbols()
    {
        ImmutableList.Builder<Symbol> inputSymbols = ImmutableList.builder();

        for (ValuePointer valuePointer : valuePointers) {
            if (valuePointer instanceof ScalarValuePointer pointer) {
                Symbol symbol = pointer.getInputSymbol();
                if (!classifierSymbols.contains(symbol) && !matchNumberSymbols.contains(symbol)) {
                    inputSymbols.add(symbol);
                }
            }
            else if (valuePointer instanceof AggregationValuePointer) {
                inputSymbols.addAll(((AggregationValuePointer) valuePointer).getInputSymbols());
            }
            else {
                throw new UnsupportedOperationException("unexpected ValuePointer type: " + valuePointer.getClass().getSimpleName());
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
                Objects.equals(layout, o.layout) &&
                Objects.equals(valuePointers, o.valuePointers) &&
                Objects.equals(classifierSymbols, o.classifierSymbols) &&
                Objects.equals(matchNumberSymbols, o.matchNumberSymbols);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, layout, valuePointers, classifierSymbols, matchNumberSymbols);
    }
}
