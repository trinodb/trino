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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.ir.IrUtils.validateType;

@JsonSerialize
public record Coalesce(List<Expression> operands)
        implements Expression
{
    public Coalesce(Expression first, Expression second, Expression... additional)
    {
        this(ImmutableList.<Expression>builder()
                .add(first, second)
                .add(additional)
                .build());
    }

    @Override
    public Type type()
    {
        return operands.getFirst().type();
    }

    public Coalesce
    {
        checkArgument(operands.size() >= 2, "must have at least two operands");
        operands = ImmutableList.copyOf(operands);

        for (int i = 1; i < operands.size(); i++) {
            validateType(operands.getFirst().type(), operands.get(i));
        }
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCoalesce(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return operands;
    }
}
