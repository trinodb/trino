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
package io.trino.sql.relational;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record CallExpression(ResolvedFunction resolvedFunction, List<RowExpression> arguments)
        implements RowExpression
{
    @JsonCreator
    public CallExpression
    {
        requireNonNull(resolvedFunction, "resolvedFunction is null");
        requireNonNull(arguments, "arguments is null");
        arguments = ImmutableList.copyOf(arguments);
    }

    @Override
    public Type type()
    {
        return resolvedFunction.signature().getReturnType();
    }

    @Override
    public String toString()
    {
        return resolvedFunction.signature().getName() + "(" + Joiner.on(", ").join(arguments) + ")";
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
