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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class CanonicalAggregation
        extends Expression
{
    private final ResolvedFunction resolvedFunction;
    private final Optional<Symbol> mask;
    private final List<Expression> arguments;

    public CanonicalAggregation(ResolvedFunction resolvedFunction, Optional<Symbol> mask, List<Expression> arguments)
    {
        this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
        this.mask = requireNonNull(mask, "mask is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public ResolvedFunction getResolvedFunction()
    {
        return resolvedFunction;
    }

    public Optional<Symbol> getMask()
    {
        return mask;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return arguments;
    }

    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCanonicalAggregation(this, context);
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
        CanonicalAggregation that = (CanonicalAggregation) o;
        return Objects.equals(resolvedFunction, that.resolvedFunction) &&
                Objects.equals(mask, that.mask) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resolvedFunction, mask, arguments);
    }
}
