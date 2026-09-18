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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/// A cast of an expression to a target type. [#kind] distinguishes a value-changing [Kind#CONVERT]
/// from a representation-preserving [Kind#REINTERPRET].
@JsonSerialize
public record Cast(Expression expression, Type type, Kind kind)
        implements Expression
{
    /// Distinguishes how a [Cast] relates its source and target types.
    public enum Kind
    {
        /// The source and target have different physical representations, so the cast changes the
        /// value's encoding at runtime via a coercion function and may fail.
        CONVERT,

        /// The source and target share the same physical representation, so the cast is a runtime
        /// no-op. Corresponds to `TypeCoercion.isTypeOnlyCoercion(source, target)`.
        REINTERPRET,
    }

    @JsonCreator
    public Cast(
            @JsonProperty("expression") Expression expression,
            @JsonProperty("type") Type type,
            @JsonProperty("kind") Kind kind)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.type = requireNonNull(type, "type is null");
        this.kind = requireNonNull(kind, "kind is null");
    }

    /// Creates a [Kind#CONVERT] cast (the common case).
    public Cast(Expression expression, Type type)
    {
        this(expression, type, Kind.CONVERT);
    }

    @Override
    public Type type()
    {
        return type;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public String toString()
    {
        return "Cast(%s, %s%s)".formatted(expression, type, kind == Kind.REINTERPRET ? ", reinterpret" : "");
    }
}
