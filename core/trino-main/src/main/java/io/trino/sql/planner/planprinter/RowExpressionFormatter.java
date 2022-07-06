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
package io.trino.sql.planner.planprinter;

import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.sql.planner.LiteralInterpreter;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.StandardFunctionResolution;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.List;
import java.util.Locale;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.Signature.unmangleOperator;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class RowExpressionFormatter
{
    public RowExpressionFormatter()
    {
    }

    public String formatRowExpression(ConnectorSession session, RowExpression expression)
    {
        return expression.accept(new Formatter(), requireNonNull(session, "session is null"));
    }

    private List<String> formatRowExpressions(ConnectorSession session, List<RowExpression> rowExpressions)
    {
        return rowExpressions.stream().map(rowExpression -> formatRowExpression(session, rowExpression)).collect(toList());
    }

    public class Formatter
            implements RowExpressionVisitor<String, ConnectorSession>
    {
        @Override
        public String visitCall(CallExpression node, ConnectorSession session)
        {
            if (StandardFunctionResolution.isArithmeticFunction(node.getResolvedFunction()) || StandardFunctionResolution.isComparisonFunction(node.getResolvedFunction())) {
                String operation = unmangleOperator(node.getResolvedFunction().getSignature().getName()).getOperator();
                return String.join(" " + operation + " ", formatRowExpressions(session, node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
            }
            else if (StandardFunctionResolution.isCastFunction(node.getResolvedFunction())) {
                return String.format("CAST(%s AS %s)", formatRowExpression(session, node.getArguments().get(0)), node.getType().getDisplayName());
            }
            else if (StandardFunctionResolution.isNegateFunction(node.getResolvedFunction())) {
                return "-(" + formatRowExpression(session, node.getArguments().get(0)) + ")";
            }
            else if (StandardFunctionResolution.isSubscriptFunction(node.getResolvedFunction())) {
                return formatRowExpression(session, node.getArguments().get(0)) + "[" + formatRowExpression(session, node.getArguments().get(1)) + "]";
            }
            String operation = unmangleOperator(node.getResolvedFunction().getSignature().getName()).getOperator();
            return operation + "(" + String.join(", ", formatRowExpressions(session, node.getArguments())) + ")";
        }

        @Override
        public String visitSpecialForm(SpecialForm node, ConnectorSession session)
        {
            if (node.getForm().equals(SpecialForm.Form.AND) || node.getForm().equals(SpecialForm.Form.OR)) {
                return String.join(" " + node.getForm() + " ", formatRowExpressions(session, node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
            }
            return node.getForm().name() + "(" + String.join(", ", formatRowExpressions(session, node.getArguments())) + ")";
        }

        @Override
        public String visitInputReference(InputReferenceExpression node, ConnectorSession session)
        {
            return node.toString();
        }

        @Override
        public String visitLambda(LambdaDefinitionExpression node, ConnectorSession session)
        {
            return "(" + String.join(", ", node.getArguments()) + ") -> " + formatRowExpression(session, node.getBody());
        }

        @Override
        public String visitVariableReference(VariableReferenceExpression node, ConnectorSession session)
        {
            return node.getName();
        }

        @Override
        public String visitConstant(ConstantExpression node, ConnectorSession session)
        {
            Object value = LiteralInterpreter.evaluate(node);

            if (value == null) {
                return String.valueOf((Object) null);
            }

            Type type = node.getType();
            if (type.getJavaType() == Block.class) {
                Block block = (Block) value;
                // TODO: format block
                return format("[Block: position count: %s; size: %s bytes]", block.getPositionCount(), block.getRetainedSizeInBytes());
            }

            String valueString = "'" + value.toString().replace("'", "''") + "'";

            if (type instanceof VarbinaryType) {
                return "X" + valueString;
            }

            return type.getTypeSignature().getBase().toUpperCase(Locale.ENGLISH) + valueString;
        }
    }
}
