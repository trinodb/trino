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
package io.trino.sql.tree;

import com.google.errorprone.annotations.Immutable;
import io.trino.sql.ExpressionFormatter;

import java.util.Optional;

@Immutable
public abstract sealed class Expression
        extends Node
        permits AllRows,
                ArithmeticBinaryExpression,
                ArithmeticUnaryExpression,
                Array,
                ArrayWildcardSubscript,
                AtLocal,
                AtTimeZone,
                Cast,
                CoalesceExpression,
                CurrentCatalog,
                CurrentDate,
                CurrentPath,
                CurrentSchema,
                CurrentTime,
                CurrentTimestamp,
                CurrentUser,
                DataType,
                DereferenceExpression,
                ExistsPredicate,
                Extract,
                FieldReference,
                Format,
                FunctionCall,
                GroupingOperation,
                Identifier,
                IfExpression,
                InListExpression,
                IntervalValueExpression,
                JsonArray,
                JsonConstructor,
                JsonExists,
                JsonObject,
                JsonQuery,
                JsonSerialize,
                JsonValue,
                LambdaArgumentDeclaration,
                LambdaExpression,
                Literal,
                LocalTime,
                LocalTimestamp,
                LogicalExpression,
                MethodCall,
                NotExpression,
                NullIfExpression,
                Overlay,
                Parameter,
                Predicated,
                Row,
                SearchedCaseExpression,
                SimpleCaseExpression,
                StaticMethodCall,
                SubqueryExpression,
                SubscriptExpression,
                Trim,
                TryExpression,
                UniquePredicate,
                WhenClause,
                WindowOperation
{
    @Deprecated
    protected Expression(Optional<NodeLocation> location)
    {
        super(location);
    }

    protected Expression(NodeLocation location)
    {
        super(location);
    }

    @Override
    public final String toString()
    {
        return ExpressionFormatter.formatExpression(this);
    }
}
