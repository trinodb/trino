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
import io.trino.sql.ExpressionFormatter;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AllRows.class, name = "allRows"),
        @JsonSubTypes.Type(value = ArithmeticBinaryExpression.class, name = "arithmeticBinaryExpression"),
        @JsonSubTypes.Type(value = ArithmeticUnaryExpression.class, name = "arithmeticUnaryExpression"),
        @JsonSubTypes.Type(value = AtTimeZone.class, name = "atTimeZone"),
        @JsonSubTypes.Type(value = BetweenPredicate.class, name = "betweenPredicate"),
        @JsonSubTypes.Type(value = BinaryLiteral.class, name = "binaryLiteral"),
        @JsonSubTypes.Type(value = BooleanLiteral.class, name = "booleanLiteral"),
        @JsonSubTypes.Type(value = Cast.class, name = "cast"),
        @JsonSubTypes.Type(value = CharLiteral.class, name = "charLiteral"),
        @JsonSubTypes.Type(value = CoalesceExpression.class, name = "coalesceExpression"),
        @JsonSubTypes.Type(value = ComparisonExpression.class, name = "comparisonExpression"),
        @JsonSubTypes.Type(value = CurrentCatalog.class, name = "currentCatalog"),
        @JsonSubTypes.Type(value = CurrentPath.class, name = "currentPath"),
        @JsonSubTypes.Type(value = CurrentSchema.class, name = "currentSchema"),
        @JsonSubTypes.Type(value = CurrentTime.class, name = "currentTime"),
        @JsonSubTypes.Type(value = CurrentUser.class, name = "currentUser"),
        @JsonSubTypes.Type(value = DataType.class, name = "dataType"),
        @JsonSubTypes.Type(value = DateTimeDataType.class, name = "dateTimeDataType"),
        @JsonSubTypes.Type(value = DecimalLiteral.class, name = "decimalLiteral"),
        @JsonSubTypes.Type(value = DereferenceExpression.class, name = "dereferenceExpression"),
        @JsonSubTypes.Type(value = DoubleLiteral.class, name = "doubleLiteral"),
        @JsonSubTypes.Type(value = ExistsPredicate.class, name = "existsPredicate"),
        @JsonSubTypes.Type(value = Extract.class, name = "extract"),
        @JsonSubTypes.Type(value = Format.class, name = "format"),
        @JsonSubTypes.Type(value = FunctionCall.class, name = "functionCall"),
        @JsonSubTypes.Type(value = GenericDataType.class, name = "genericDataType"),
        @JsonSubTypes.Type(value = GenericLiteral.class, name = "genericLiteral"),
        @JsonSubTypes.Type(value = GroupingOperation.class, name = "groupingOperation"),
        @JsonSubTypes.Type(value = Identifier.class, name = "identifier"),
        @JsonSubTypes.Type(value = IfExpression.class, name = "ifExpression"),
        @JsonSubTypes.Type(value = InListExpression.class, name = "inListExpression"),
        @JsonSubTypes.Type(value = InPredicate.class, name = "inPredicate"),
        @JsonSubTypes.Type(value = IntervalDayTimeDataType.class, name = "intervalDayTimeDataType"),
        @JsonSubTypes.Type(value = IsNotNullPredicate.class, name = "isNotNullPredicate"),
        @JsonSubTypes.Type(value = IsNullPredicate.class, name = "isNullPredicate"),
        @JsonSubTypes.Type(value = JsonArray.class, name = "jsonArray"),
        @JsonSubTypes.Type(value = JsonExists.class, name = "jsonExists"),
        @JsonSubTypes.Type(value = JsonObject.class, name = "jsonObject"),
        @JsonSubTypes.Type(value = JsonQuery.class, name = "jsonQuery"),
        @JsonSubTypes.Type(value = JsonValue.class, name = "jsonValue"),
        @JsonSubTypes.Type(value = LambdaArgumentDeclaration.class, name = "lambdaArgumentDeclaration"),
        @JsonSubTypes.Type(value = LambdaExpression.class, name = "lambdaExpression"),
        @JsonSubTypes.Type(value = LikePredicate.class, name = "likePredicate"),
        @JsonSubTypes.Type(value = Literal.class, name = "literal"),
        @JsonSubTypes.Type(value = LogicalExpression.class, name = "logicalExpression"),
        @JsonSubTypes.Type(value = LongLiteral.class, name = "longLiteral"),
        @JsonSubTypes.Type(value = NotExpression.class, name = "notExpression"),
        @JsonSubTypes.Type(value = NullIfExpression.class, name = "nullIfExpression"),
        @JsonSubTypes.Type(value = NullLiteral.class, name = "nullLiteral"),
        @JsonSubTypes.Type(value = Parameter.class, name = "parameter"),
        @JsonSubTypes.Type(value = QuantifiedComparisonExpression.class, name = "quantifiedComparisonExpression"),
        @JsonSubTypes.Type(value = Row.class, name = "row"),
        @JsonSubTypes.Type(value = RowDataType.class, name = "rowDataType"),
        @JsonSubTypes.Type(value = SearchedCaseExpression.class, name = "searchedCaseExpression"),
        @JsonSubTypes.Type(value = SimpleCaseExpression.class, name = "simpleCaseExpression"),
        @JsonSubTypes.Type(value = StringLiteral.class, name = "stringLiteral"),
        @JsonSubTypes.Type(value = SubqueryExpression.class, name = "subqueryExpression"),
        @JsonSubTypes.Type(value = SubscriptExpression.class, name = "subscriptExpression"),
        @JsonSubTypes.Type(value = SymbolReference.class, name = "symbolReference"),
        @JsonSubTypes.Type(value = TimeLiteral.class, name = "timeLiteral"),
        @JsonSubTypes.Type(value = TimestampLiteral.class, name = "timestampLiteral"),
        @JsonSubTypes.Type(value = Trim.class, name = "trim"),
        @JsonSubTypes.Type(value = TryExpression.class, name = "tryExpression"),
        @JsonSubTypes.Type(value = WhenClause.class, name = "whenClause"),
        @JsonSubTypes.Type(value = WindowOperation.class, name = "windowOperation")})
public abstract class Expression
        extends Node
{
    protected Expression()
    {
    }

    /**
     * Accessible for {@link IrVisitor}, use {@link IrVisitor#process(Node, Object)} instead.
     */
    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString()
    {
        return ExpressionFormatter.formatExpression(this);
    }
}
