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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrUtils.validateType;
import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Case(List<WhenClause> whenClauses, Expression defaultValue)
        implements Expression
{
    public Case
    {
        checkArgument(!whenClauses.isEmpty(), "whenClauses is empty");
        whenClauses = ImmutableList.copyOf(whenClauses);
        requireNonNull(defaultValue, "defaultValue is null");

        for (WhenClause clause : whenClauses) {
            validateType(BOOLEAN, clause.getOperand());
            validateType(defaultValue.type(), clause.getResult());
        }
    }

    @Override
    public Type type()
    {
        return whenClauses.getFirst().getResult().type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCase(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        whenClauses.forEach(clause -> {
            builder.add(clause.getOperand());
            builder.add(clause.getResult());
        });

        builder.add(defaultValue);

        return builder.build();
    }

    @Override
    public String toString()
    {
        return "Case(%s, %s)".formatted(
                whenClauses.stream()
                        .map(WhenClause::toString)
                        .collect(Collectors.joining(", ")),
                defaultValue);
    }
}
