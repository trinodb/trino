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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.type.Type;

import java.util.List;

@Immutable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Array.class, name = "array"),
        @JsonSubTypes.Type(value = Between.class, name = "between"),
        @JsonSubTypes.Type(value = Bind.class, name = "bind"),
        @JsonSubTypes.Type(value = Cast.class, name = "cast"),
        @JsonSubTypes.Type(value = Coalesce.class, name = "coalesce"),
        @JsonSubTypes.Type(value = Comparison.class, name = "comparison"),
        @JsonSubTypes.Type(value = Call.class, name = "call"),
        @JsonSubTypes.Type(value = Constant.class, name = "constant"),
        @JsonSubTypes.Type(value = In.class, name = "in"),
        @JsonSubTypes.Type(value = IsNull.class, name = "isnull"),
        @JsonSubTypes.Type(value = Lambda.class, name = "lambda"),
        @JsonSubTypes.Type(value = Logical.class, name = "logical"),
        @JsonSubTypes.Type(value = Not.class, name = "not"),
        @JsonSubTypes.Type(value = NullIf.class, name = "nullif"),
        @JsonSubTypes.Type(value = Row.class, name = "row"),
        @JsonSubTypes.Type(value = Case.class, name = "case"),
        @JsonSubTypes.Type(value = Switch.class, name = "switch"),
        @JsonSubTypes.Type(value = FieldReference.class, name = "field"),
        @JsonSubTypes.Type(value = Reference.class, name = "reference"),
})
public sealed interface Expression
        permits Array, Between, Bind, Call, Case, Cast, Coalesce,
        Comparison, Constant, FieldReference, In, IsNull, Lambda, Logical,
        Not, NullIf, Reference, Row, Switch
{
    Type type();

    /**
     * Accessible for {@link IrVisitor}, use {@link IrVisitor#process(Expression, Object)} instead.
     */
    default <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    @JsonIgnore
    List<? extends Expression> children();
}
