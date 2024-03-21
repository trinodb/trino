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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Switch(Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
        implements Expression
{
    public Switch
    {
        requireNonNull(operand, "operand is null");
        whenClauses = ImmutableList.copyOf(whenClauses);
        requireNonNull(defaultValue, "defaultValue is null");
    }

    @Override
    public Type type()
    {
        return whenClauses.getFirst().getResult().type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSwitch(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.<Expression>builder()
                .add(operand);

        whenClauses.forEach(clause -> {
            builder.add(clause.getOperand());
            builder.add(clause.getResult());
        });

        defaultValue.ifPresent(builder::add);

        return builder.build();
    }
}
