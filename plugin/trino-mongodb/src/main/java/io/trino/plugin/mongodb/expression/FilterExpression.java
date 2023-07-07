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
package io.trino.plugin.mongodb.expression;

import jakarta.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public record FilterExpression(@Nullable Object expression, ExpressionType expressionType, Optional<ExpressionInfo> expressionInfo)
{
    public enum ExpressionType
    {
        LITERAL,
        DOCUMENT,
        VARIABLE,
    }

    public FilterExpression(@Nullable Object expression, ExpressionType expressionType)
    {
        this(expression, expressionType, Optional.empty());
    }

    public FilterExpression(@Nullable Object expression, ExpressionType expressionType, Optional<ExpressionInfo> expressionInfo)
    {
        this.expression = expression;
        this.expressionType = requireNonNull(expressionType, "expressionType is null");
        this.expressionInfo = requireNonNull(expressionInfo, "expressionInfo is null");
        if (expressionType == ExpressionType.VARIABLE) {
            verify(expressionInfo.isPresent(), "expressionInfo is mandatory when expressionType is VARIABLE");
        }
    }

    public boolean isLiteral()
    {
        return expressionType == ExpressionType.LITERAL;
    }

    public boolean isDocument()
    {
        return expressionType == ExpressionType.DOCUMENT;
    }

    public boolean isVariable()
    {
        return expressionType == ExpressionType.VARIABLE;
    }
}
