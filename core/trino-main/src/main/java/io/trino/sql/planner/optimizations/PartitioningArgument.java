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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PartitioningArgument
{
    private final Expression expression;
    private final NullableValue constant;

    @JsonCreator
    public PartitioningArgument(
            @JsonProperty("expression") Expression expression,
            @JsonProperty("constant") NullableValue constant)
    {
        this.expression = expression;
        this.constant = constant;
        checkArgument((expression == null) != (constant == null), "Either expression or constant must be set");
    }

    public static PartitioningArgument expressionArgument(Expression expression)
    {
        return new PartitioningArgument(requireNonNull(expression, "expression is null"), null);
    }

    public static PartitioningArgument constantArgument(NullableValue constant)
    {
        return new PartitioningArgument(null, requireNonNull(constant, "constant is null"));
    }

    public boolean isConstant()
    {
        return constant != null;
    }

    public boolean isVariable()
    {
        return expression instanceof SymbolReference;
    }

    public Symbol getColumn()
    {
        verify(expression instanceof SymbolReference, "Expect the expression to be a SymbolReference");
        return Symbol.from(expression);
    }

    @JsonProperty
    public Expression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public NullableValue getConstant()
    {
        return constant;
    }

    public PartitioningArgument translate(Function<Symbol, Symbol> translator)
    {
        if (isConstant()) {
            return this;
        }
        return expressionArgument(translator.apply(Symbol.from(expression)).toSymbolReference());
    }

    public Optional<PartitioningArgument> translate(Translator translator)
    {
        if (isConstant()) {
            return Optional.of(this);
        }

        if (!isVariable()) {
            return translator.expressionTranslator.apply(expression)
                    .map(Symbol::toSymbolReference)
                    .map(PartitioningArgument::expressionArgument);
        }

        Optional<PartitioningArgument> newColumn = translator.columnTranslator.apply(Symbol.from(expression))
                .map(Symbol::toSymbolReference)
                .map(PartitioningArgument::expressionArgument);
        if (newColumn.isPresent()) {
            return newColumn;
        }
        // As a last resort, check for a constant mapping for the symbol
        // Note: this MUST be last because we want to favor the symbol representation
        // as it makes further optimizations possible.
        return translator.constantTranslator.apply(Symbol.from(expression))
                .map(PartitioningArgument::constantArgument);
    }

    @Override
    public String toString()
    {
        if (constant != null) {
            return constant.toString();
        }

        return expression.toString();
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
        PartitioningArgument that = (PartitioningArgument) o;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(constant, that.constant);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, constant);
    }

    @Immutable
    public static final class Translator
    {
        private final Function<Symbol, Optional<Symbol>> columnTranslator;
        private final Function<Symbol, Optional<NullableValue>> constantTranslator;
        private final Function<Expression, Optional<Symbol>> expressionTranslator;

        public Translator(
                Function<Symbol, Optional<Symbol>> columnTranslator,
                Function<Symbol, Optional<NullableValue>> constantTranslator,
                Function<Expression, Optional<Symbol>> expressionTranslator)
        {
            this.columnTranslator = requireNonNull(columnTranslator, "columnTranslator is null");
            this.constantTranslator = requireNonNull(constantTranslator, "constantTranslator is null");
            this.expressionTranslator = requireNonNull(expressionTranslator, "expressionTranslator is null");
        }
    }
}
