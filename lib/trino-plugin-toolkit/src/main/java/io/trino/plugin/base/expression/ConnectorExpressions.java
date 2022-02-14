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
package io.trino.plugin.base.expression;

import com.google.common.collect.ImmutableList;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.LogicalExpression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressions
{
    private ConnectorExpressions() {}

    public static ConnectorExpression and(ConnectorExpression left, ConnectorExpression right)
    {
        return and(ImmutableList.of(left, right));
    }

    public static ConnectorExpression and(List<ConnectorExpression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<ConnectorExpression> terms = expressions.stream()
                .map(ConnectorExpressions::extractConjuncts)
                .flatMap(List::stream)
                .filter(expression -> !expression.equals(Constant.TRUE))
                .collect(toImmutableList());

        if (terms.isEmpty()) {
            return Constant.TRUE;
        }
        if (terms.size() == 1) {
            return getOnlyElement(terms);
        }
        return new LogicalExpression(LogicalExpression.Operator.AND, terms);
    }

    public static List<ConnectorExpression> extractConjuncts(ConnectorExpression expression)
    {
        return extractPredicates(LogicalExpression.Operator.AND, expression);
    }

    private static List<ConnectorExpression> extractPredicates(LogicalExpression.Operator operator, ConnectorExpression expression)
    {
        ImmutableList.Builder<ConnectorExpression> result = ImmutableList.builder();
        extractPredicates(result, operator, expression);
        return result.build();
    }

    private static void extractPredicates(ImmutableList.Builder<ConnectorExpression> result, LogicalExpression.Operator operator, ConnectorExpression expression)
    {
        if (expression instanceof LogicalExpression) {
            LogicalExpression logicalExpression = (LogicalExpression) expression;
            if (logicalExpression.getOperator() == operator) {
                for (ConnectorExpression term : logicalExpression.getTerms()) {
                    extractPredicates(result, operator, term);
                }
                return;
            }
        }
        result.add(expression);
    }
}
