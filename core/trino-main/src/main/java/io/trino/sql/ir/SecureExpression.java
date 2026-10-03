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

import static java.util.Objects.requireNonNull;

/**
 * Marks an access-control expression whose text must not be exposed through query details.
 * Semantically, this expression is identical to the wrapped expression.
 */
@JsonSerialize
public record SecureExpression(Expression expression)
        implements Expression
{
    public static final String REDACTED = "[REDACTED]";

    public SecureExpression
    {
        requireNonNull(expression, "expression is null");
    }

    @Override
    public Type type()
    {
        return expression.type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSecureExpression(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public String toString()
    {
        return REDACTED;
    }
}
