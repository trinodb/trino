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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BinaryLiteral.class, name = "binaryLiteral"),
        @JsonSubTypes.Type(value = BooleanLiteral.class, name = "booleanLiteral"),
        @JsonSubTypes.Type(value = CharLiteral.class, name = "charLiteral"),
        @JsonSubTypes.Type(value = DecimalLiteral.class, name = "decimalLiteral"),
        @JsonSubTypes.Type(value = DoubleLiteral.class, name = "doubleLiteral"),
        @JsonSubTypes.Type(value = GenericLiteral.class, name = "genericLiteral"),
        @JsonSubTypes.Type(value = IntervalLiteral.class, name = "intervalLiteral"),
        @JsonSubTypes.Type(value = LongLiteral.class, name = "longLiteral"),
        @JsonSubTypes.Type(value = NullLiteral.class, name = "nullLiteral"),
        @JsonSubTypes.Type(value = StringLiteral.class, name = "stringLiteral"),
        @JsonSubTypes.Type(value = TimeLiteral.class, name = "timeLiteral"),
        @JsonSubTypes.Type(value = TimestampLiteral.class, name = "timestampLiteral")
})
public abstract class Literal
        extends Expression
{
    protected Literal()
    {
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public List<Expression> getChildren()
    {
        return ImmutableList.of();
    }
}
