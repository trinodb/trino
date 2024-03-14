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
import com.google.errorprone.annotations.Immutable;

import java.util.List;

@Immutable
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ArithmeticBinaryExpression.class, name = "arithmeticBinary"),
        @JsonSubTypes.Type(value = ArithmeticUnaryExpression.class, name = "arithmeticUnary"),
        @JsonSubTypes.Type(value = Array.class, name = "array"),
        @JsonSubTypes.Type(value = BetweenPredicate.class, name = "between"),
        @JsonSubTypes.Type(value = BinaryLiteral.class, name = "binary"),
        @JsonSubTypes.Type(value = BindExpression.class, name = "bind"),
        @JsonSubTypes.Type(value = BooleanLiteral.class, name = "boolean"),
        @JsonSubTypes.Type(value = Cast.class, name = "cast"),
        @JsonSubTypes.Type(value = CoalesceExpression.class, name = "coalesce"),
        @JsonSubTypes.Type(value = ComparisonExpression.class, name = "comparison"),
        @JsonSubTypes.Type(value = DecimalLiteral.class, name = "decimal"),
        @JsonSubTypes.Type(value = DoubleLiteral.class, name = "double"),
        @JsonSubTypes.Type(value = FunctionCall.class, name = "call"),
        @JsonSubTypes.Type(value = GenericLiteral.class, name = "constant"),
        @JsonSubTypes.Type(value = IfExpression.class, name = "if"),
        @JsonSubTypes.Type(value = InPredicate.class, name = "in"),
        @JsonSubTypes.Type(value = IntervalLiteral.class, name = "interval"),
        @JsonSubTypes.Type(value = IsNotNullPredicate.class, name = "isNotNull"),
        @JsonSubTypes.Type(value = IsNullPredicate.class, name = "isNull"),
        @JsonSubTypes.Type(value = LambdaExpression.class, name = "lambda"),
        @JsonSubTypes.Type(value = LogicalExpression.class, name = "logicalBinary"),
        @JsonSubTypes.Type(value = LongLiteral.class, name = "long"),
        @JsonSubTypes.Type(value = NotExpression.class, name = "not"),
        @JsonSubTypes.Type(value = NullIfExpression.class, name = "nullif"),
        @JsonSubTypes.Type(value = NullLiteral.class, name = "null"),
        @JsonSubTypes.Type(value = Row.class, name = "row"),
        @JsonSubTypes.Type(value = SearchedCaseExpression.class, name = "searchedCase"),
        @JsonSubTypes.Type(value = SimpleCaseExpression.class, name = "simpleCase"),
        @JsonSubTypes.Type(value = SubscriptExpression.class, name = "subscript"),
        @JsonSubTypes.Type(value = StringLiteral.class, name = "string"),
        @JsonSubTypes.Type(value = SymbolReference.class, name = "symbol"),
        @JsonSubTypes.Type(value = WhenClause.class, name = "when"),
})
public abstract sealed class Expression
        permits ArithmeticBinaryExpression, ArithmeticUnaryExpression, Array, BetweenPredicate,
        BindExpression, Cast, CoalesceExpression, ComparisonExpression, FunctionCall,
        IfExpression, InPredicate, IsNotNullPredicate, IsNullPredicate,
        LambdaExpression, Literal, LogicalExpression,
        NotExpression, NullIfExpression, Row, SearchedCaseExpression, SimpleCaseExpression,
        SubscriptExpression, SymbolReference, WhenClause
{
    /**
     * Accessible for {@link IrVisitor}, use {@link IrVisitor#process(Expression, Object)} instead.
     */
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    public abstract List<? extends Expression> getChildren();
}
