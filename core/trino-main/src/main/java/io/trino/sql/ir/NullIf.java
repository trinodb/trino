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
 * NULLIF(V1,V2): CASE WHEN V1=V2 THEN NULL ELSE V1 END
 */
@JsonSerialize
public record NullIf(Expression first, Expression second)
        implements Expression
{
    public NullIf
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");

        // TODO: verify that first and second can be coerced to the same type
    }

    @Override
    public Type type()
    {
        return first.type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullIf(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(first, second);
    }

    @Override
    public String toString()
    {
        return "NullIf(%s, %s)".formatted(first, second);
    }
}
