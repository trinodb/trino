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
package io.trino.sql.planner;

import io.trino.sql.tree.Expression;
import io.trino.sql.util.AstUtils;

import java.util.stream.Stream;

/**
 * Extracts and returns the stream of all expression subtrees within an Expression, including Expression itself
 */
public final class SubExpressionExtractor
{
    private SubExpressionExtractor() {}

    public static Stream<Expression> extract(Expression expression)
    {
        return AstUtils.preOrder(expression)
                .filter(Expression.class::isInstance)
                .map(Expression.class::cast);
    }
}
