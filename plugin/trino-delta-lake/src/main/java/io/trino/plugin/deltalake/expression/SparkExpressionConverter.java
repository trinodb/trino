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
package io.trino.plugin.deltalake.expression;

public final class SparkExpressionConverter
{
    private SparkExpressionConverter() {}

    public static String toTrinoExpression(SparkExpression expression)
    {
        return new Formatter().process(expression, null);
    }

    private static class Formatter
            extends SparkExpressionTreeVisitor<String, Void>
    {
        @Override
        protected String visitExpression(SparkExpression node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return "(%s %s %s)".formatted(process(node.getLeft(), context), node.getOperator().getValue(), process(node.getRight(), context));
        }

        @Override
        protected String visitLogicalExpression(LogicalExpression node, Void context)
        {
            return "(%s %s %s)".formatted(process(node.getLeft(), context), node.getOperator().toString(), process(node.getRight(), context));
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            return '"' + node.getValue().replace("\"", "\"\"") + '"';
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return Boolean.toString(node.getValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return "'" + node.getValue().replace("'", "''") + "'";
        }
    }
}
