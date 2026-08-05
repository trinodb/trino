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
import io.trino.sql.planner.Symbol;

import java.util.List;

import static io.trino.sql.ir.IrUtils.validateType;

/**
 * Let(name, value, body)
 * <p>
 * Evaluates value, binds it to name, and evaluates body with the binding in scope.
 * The result is the value of body. Use to avoid duplicating a subexpression that is
 * referenced multiple times in the body — particularly important when the
 * subexpression is non-deterministic, where structural sharing in the IR tree would
 * still result in repeated evaluation.
 */
@JsonSerialize
public record Let(Symbol name, Expression value, Expression body)
        implements Expression
{
    public Let
    {
        validateType(name.type(), value);
    }

    @Override
    public Type type()
    {
        return body.type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLet(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(value, body);
    }

    @Override
    public String toString()
    {
        return "Let(%s = %s, %s)".formatted(name, value, body);
    }
}
