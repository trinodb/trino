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
package io.prestosql.sql.relational;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class CallExpression
        extends RowExpression
{
    private final ResolvedFunction resolvedFunction;
    private final Type returnType;
    private final List<RowExpression> arguments;

    public CallExpression(ResolvedFunction resolvedFunction, Type returnType, List<RowExpression> arguments)
    {
        requireNonNull(resolvedFunction, "resolvedFunction is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(returnType, "returnType is null");

        this.resolvedFunction = resolvedFunction;
        this.returnType = returnType;
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public ResolvedFunction getResolvedFunction()
    {
        return resolvedFunction;
    }

    @Override
    public Type getType()
    {
        return returnType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return resolvedFunction.getSignature().getName() + "(" + Joiner.on(", ").join(arguments) + ")";
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
        CallExpression that = (CallExpression) o;
        return Objects.equals(resolvedFunction, that.resolvedFunction) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resolvedFunction, returnType, arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
