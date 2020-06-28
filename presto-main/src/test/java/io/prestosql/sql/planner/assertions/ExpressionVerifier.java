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
package io.prestosql.sql.planner.assertions;

import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.WhenClause;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.ExpressionTestUtils.getFunctionName;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.lang.String.format;

public final class ExpressionVerifier
{
    private ExpressionVerifier() {}

    public static boolean verify(String actual, String expected, SymbolAliases symbolAliases)
    {
        return verify(expression(actual), expression(expected), symbolAliases);
    }

    public static boolean verify(Expression actual, Expression expected, SymbolAliases symbolAliases)
    {
        return new Visitor().process(actual, new Context(expected, symbolAliases));
    }

    /**
     * Expression visitor which verifies if given expression (actual) is matching other expression given as context (expected).
     * Visitor returns true if plans match to each other.
     * <p/>
     * Note that actual expression is using real name references (table columns etc) while expected expression is using symbol aliases.
     * Given symbol alias can point only to one real name reference.
     * <p/>
     * Example:
     * <pre>
     * NOT (orderkey = 3 AND custkey = 3 AND orderkey < 10)
     * </pre>
     * will match to:
     * <pre>
     * NOT (X = 3 AND Y = 3 AND X < 10)
     * </pre>
     * , but will not match to:
     * <pre>
     * NOT (X = 3 AND Y = 3 AND Z < 10)
     * </pre>
     * nor  to
     * <pre>
     * NOT (X = 3 AND X = 3 AND X < 10)
     * </pre>
     */
    private static class Visitor
            extends AstVisitor<Boolean, Context>
    {
        @Override
        protected Boolean visitNode(Node node, Context context)
        {
            throw new IllegalStateException(format("Node %s is not supported", node.getClass().getSimpleName()));
        }

        @Override
        protected Boolean visitGenericLiteral(GenericLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof GenericLiteral)) {
                return false;
            }

            return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression)) &&
                    actual.getType().equals(((GenericLiteral) expectedExpression).getType());
        }

        @Override
        protected Boolean visitStringLiteral(StringLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof StringLiteral)) {
                return false;
            }

            StringLiteral expected = (StringLiteral) expectedExpression;

            return actual.getValue().equals(expected.getValue());
        }

        @Override
        protected Boolean visitLongLiteral(LongLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof LongLiteral)) {
                return false;
            }

            return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
        }

        @Override
        protected Boolean visitDoubleLiteral(DoubleLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof DoubleLiteral)) {
                return false;
            }

            return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
        }

        @Override
        protected Boolean visitDecimalLiteral(DecimalLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof DecimalLiteral)) {
                return false;
            }

            return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
        }

        @Override
        protected Boolean visitBooleanLiteral(BooleanLiteral actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof BooleanLiteral)) {
                return false;
            }

            return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
        }

        @Override
        protected Boolean visitNullLiteral(NullLiteral node, Context context)
        {
            return context.getExpectedNode() instanceof NullLiteral;
        }

        private static String getValueFromLiteral(Node expression)
        {
            if (expression instanceof LongLiteral) {
                return String.valueOf(((LongLiteral) expression).getValue());
            }

            if (expression instanceof BooleanLiteral) {
                return String.valueOf(((BooleanLiteral) expression).getValue());
            }

            if (expression instanceof DoubleLiteral) {
                return String.valueOf(((DoubleLiteral) expression).getValue());
            }

            if (expression instanceof DecimalLiteral) {
                return String.valueOf(((DecimalLiteral) expression).getValue());
            }

            if (expression instanceof GenericLiteral) {
                return ((GenericLiteral) expression).getValue();
            }

            throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
        }

        @Override
        protected Boolean visitSymbolReference(SymbolReference actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof SymbolReference)) {
                return false;
            }

            SymbolReference expected = (SymbolReference) expectedExpression;

            return context.getSymbolAliases().get(expected.getName()).equals(actual);
        }

        @Override
        protected Boolean visitDereferenceExpression(DereferenceExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof DereferenceExpression)) {
                return false;
            }

            DereferenceExpression expected = (DereferenceExpression) expectedExpression;

            return actual.getField().equals(expected.getField()) &&
                    process(actual.getBase(), context.replaceExpectedNode(expected.getBase()));
        }

        @Override
        protected Boolean visitIfExpression(IfExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof IfExpression)) {
                return false;
            }

            IfExpression expected = (IfExpression) expectedExpression;

            return process(actual.getCondition(), context.replaceExpectedNode(expected.getCondition()))
                    && process(actual.getTrueValue(), context.replaceExpectedNode(expected.getTrueValue()))
                    && process(actual.getFalseValue(), expected.getFalseValue(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitCast(Cast actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof Cast)) {
                return false;
            }

            Cast expected = (Cast) expectedExpression;

            // TODO: hack!! The type in Cast is an AST structure, subject to case-sensitivity and quoting rules
            // Here we're trying to verify its IR counterpart, but the plan testing framework goes directly
            // from SQL text -> IR-like expressions without doing all the proper canonicalizations. So we cheat
            // here and normalize everything to the same case before comparing
            if (!actual.getType().toString().equalsIgnoreCase(expected.getType().toString())) {
                return false;
            }

            return process(actual.getExpression(), context.replaceExpectedNode(expected.getExpression()));
        }

        @Override
        protected Boolean visitIsNullPredicate(IsNullPredicate actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof IsNullPredicate)) {
                return false;
            }

            IsNullPredicate expected = (IsNullPredicate) expectedExpression;

            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue()));
        }

        @Override
        protected Boolean visitIsNotNullPredicate(IsNotNullPredicate actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof IsNotNullPredicate)) {
                return false;
            }

            IsNotNullPredicate expected = (IsNotNullPredicate) expectedExpression;

            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue()));
        }

        @Override
        protected Boolean visitInPredicate(InPredicate actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof InPredicate)) {
                return false;
            }

            InPredicate expected = (InPredicate) expectedExpression;

            if (actual.getValueList() instanceof InListExpression || !(expected.getValueList() instanceof InListExpression)) {
                return process(actual.getValue(), context.replaceExpectedNode(expected.getValue())) &&
                        process(actual.getValueList(), context.replaceExpectedNode(expected.getValueList()));
            }

            /*
             * In some cases, actual.getValueList() and expected.getValueList() might be of different types,
             * although they originated from identical single-element InListExpression.
             *
             * This happens because actual passes through the analyzer, planner, and possibly optimizers,
             * one of which sometimes takes the liberty of unpacking the InListExpression.
             *
             * Since the expected value doesn't go through all of that, we have to deal with the case
             * of the actual value being unpacked, but the expected value being an InListExpression.
             *
             * If the expected value is a value list, but the actual is e.g. a SymbolReference,
             * we need to unpack the value from the list to enable comparison: so that when we hit
             * visitSymbolReference, the expected.toString() call returns something that the symbolAliases
             * actually contains.
             * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
             */
            List<Expression> values = ((InListExpression) expected.getValueList()).getValues();
            checkState(values.size() == 1, "Multiple expressions in expected value list %s, but actual value is not a list", values, actual.getValue());
            Expression onlyExpectedExpression = values.get(0);
            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue())) &&
                    process(actual.getValueList(), context.replaceExpectedNode(onlyExpectedExpression));
        }

        @Override
        protected Boolean visitInListExpression(InListExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof InListExpression)) {
                return false;
            }

            InListExpression expected = (InListExpression) expectedExpression;

            return process(actual.getValues(), expected.getValues(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitComparisonExpression(ComparisonExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof ComparisonExpression)) {
                return false;
            }

            ComparisonExpression expected = (ComparisonExpression) expectedExpression;

            if (actual.getOperator() == expected.getOperator() &&
                    process(actual.getLeft(), context.replaceExpectedNode(expected.getLeft())) &&
                    process(actual.getRight(), context.replaceExpectedNode(expected.getRight()))) {
                return true;
            }

            return actual.getOperator() == expected.getOperator().flip() &&
                    process(actual.getLeft(), context.replaceExpectedNode(expected.getRight())) &&
                    process(actual.getRight(), context.replaceExpectedNode(expected.getLeft()));
        }

        @Override
        protected Boolean visitBetweenPredicate(BetweenPredicate actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof BetweenPredicate)) {
                return false;
            }

            BetweenPredicate expected = (BetweenPredicate) expectedExpression;

            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue())) &&
                    process(actual.getMin(), context.replaceExpectedNode(expected.getMin())) &&
                    process(actual.getMax(), context.replaceExpectedNode(expected.getMax()));
        }

        @Override
        protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof ArithmeticUnaryExpression)) {
                return false;
            }

            ArithmeticUnaryExpression expected = (ArithmeticUnaryExpression) expectedExpression;

            return actual.getSign() == expected.getSign() &&
                    process(actual.getValue(), context.replaceExpectedNode(expected.getValue()));
        }

        @Override
        protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof ArithmeticBinaryExpression)) {
                return false;
            }

            ArithmeticBinaryExpression expected = (ArithmeticBinaryExpression) expectedExpression;

            return actual.getOperator() == expected.getOperator() &&
                    process(actual.getLeft(), context.replaceExpectedNode(expected.getLeft())) &&
                    process(actual.getRight(), context.replaceExpectedNode(expected.getRight()));
        }

        @Override
        protected Boolean visitNotExpression(NotExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof NotExpression)) {
                return false;
            }

            NotExpression expected = (NotExpression) expectedExpression;

            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue()));
        }

        @Override
        protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof LogicalBinaryExpression)) {
                return false;
            }

            LogicalBinaryExpression expected = (LogicalBinaryExpression) expectedExpression;

            return actual.getOperator() == expected.getOperator() &&
                    process(actual.getLeft(), context.replaceExpectedNode(expected.getLeft())) &&
                    process(actual.getRight(), context.replaceExpectedNode(expected.getRight()));
        }

        @Override
        protected Boolean visitCoalesceExpression(CoalesceExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof CoalesceExpression)) {
                return false;
            }

            CoalesceExpression expected = (CoalesceExpression) expectedExpression;

            if (actual.getOperands().size() != expected.getOperands().size()) {
                return false;
            }

            for (int i = 0; i < actual.getOperands().size(); i++) {
                if (!process(actual.getOperands().get(i), context.replaceExpectedNode(expected.getOperands().get(i)))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected Boolean visitSimpleCaseExpression(SimpleCaseExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof SimpleCaseExpression)) {
                return false;
            }

            SimpleCaseExpression expected = (SimpleCaseExpression) expectedExpression;

            return process(actual.getOperand(), context.replaceExpectedNode(expected.getOperand())) &&
                    process(actual.getWhenClauses(), expected.getWhenClauses(), context.getSymbolAliases()) &&
                    process(actual.getDefaultValue(), expected.getDefaultValue(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitSearchedCaseExpression(SearchedCaseExpression actual, Context context)
        {
            Node expected = context.getExpectedNode();
            if (!(expected instanceof SearchedCaseExpression)) {
                return false;
            }

            SearchedCaseExpression expectedCase = (SearchedCaseExpression) expected;
            if (!process(actual.getWhenClauses(), expectedCase.getWhenClauses(), context.getSymbolAliases())) {
                return false;
            }

            if (actual.getDefaultValue().isPresent() != expectedCase.getDefaultValue().isPresent()) {
                return false;
            }

            return process(actual.getDefaultValue(), expectedCase.getDefaultValue(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitWhenClause(WhenClause actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof WhenClause)) {
                return false;
            }

            WhenClause expected = (WhenClause) expectedExpression;

            return process(actual.getOperand(), context.replaceExpectedNode(expected.getOperand())) &&
                    process(actual.getResult(), context.replaceExpectedNode(expected.getResult()));
        }

        @Override
        protected Boolean visitFunctionCall(FunctionCall actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof FunctionCall)) {
                return false;
            }

            FunctionCall expected = (FunctionCall) expectedExpression;

            return actual.isDistinct() == expected.isDistinct() &&
                    getFunctionName(actual).equals(getFunctionName(expected)) &&
                    process(actual.getArguments(), expected.getArguments(), context.getSymbolAliases()) &&
                    process(actual.getFilter(), expected.getFilter(), context.getSymbolAliases()) &&
                    process(actual.getWindow(), expected.getWindow(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitLambdaExpression(LambdaExpression actual, Context context)
        {
            Node expected = context.getExpectedNode();
            if (!(expected instanceof LambdaExpression)) {
                return false;
            }

            LambdaExpression lambdaExpression = (LambdaExpression) expected;

            if (actual.getArguments().size() != lambdaExpression.getArguments().size()) {
                return false;
            }

            SymbolAliases.Builder aliasesWithLambdaArguments = SymbolAliases.builder()
                    .putAll(context.getSymbolAliases());

            IntStream.range(0, actual.getArguments().size()).boxed()
                    .forEach(index -> {
                        String expectedArgument = lambdaExpression.getArguments().get(index).getName().getValue();
                        String actualArgument = actual.getArguments().get(index).getName().getValue();
                        aliasesWithLambdaArguments.put(expectedArgument, new SymbolReference(actualArgument));
                    });

            return process(actual.getBody(), new Context(lambdaExpression.getBody(), aliasesWithLambdaArguments.build()));
        }

        @Override
        protected Boolean visitArrayConstructor(ArrayConstructor actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof ArrayConstructor)) {
                return false;
            }

            ArrayConstructor expected = (ArrayConstructor) expectedExpression;

            return process(actual.getValues(), expected.getValues(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitRow(Row actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof Row)) {
                return false;
            }

            Row expected = (Row) expectedExpression;

            return process(actual.getItems(), expected.getItems(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitTryExpression(TryExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof TryExpression)) {
                return false;
            }

            TryExpression expected = (TryExpression) expectedExpression;

            return process(actual.getInnerExpression(), context.replaceExpectedNode(expected.getInnerExpression()));
        }

        @Override
        protected Boolean visitLikePredicate(LikePredicate actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof LikePredicate)) {
                return false;
            }

            LikePredicate expected = (LikePredicate) expectedExpression;

            return process(actual.getValue(), context.replaceExpectedNode(expected.getValue()))
                    && process(actual.getPattern(), context.replaceExpectedNode(expected.getPattern()))
                    && process(actual.getEscape(), expected.getEscape(), context.getSymbolAliases());
        }

        @Override
        protected Boolean visitSubscriptExpression(SubscriptExpression actual, Context context)
        {
            Node expectedExpression = context.getExpectedNode();
            if (!(expectedExpression instanceof SubscriptExpression)) {
                return false;
            }

            SubscriptExpression expected = (SubscriptExpression) expectedExpression;
            return process(actual.getBase(), context.replaceExpectedNode(expected.getBase()))
                    && process(actual.getIndex(), context.replaceExpectedNode(expected.getIndex()));
        }

        private <T extends Node> boolean process(List<T> actuals, List<T> expecteds, SymbolAliases symbolAliases)
        {
            if (actuals.size() != expecteds.size()) {
                return false;
            }
            for (int i = 0; i < actuals.size(); i++) {
                if (!process(actuals.get(i), new Context(expecteds.get(i), symbolAliases))) {
                    return false;
                }
            }
            return true;
        }

        private <T extends Node> boolean process(Optional<T> actual, Optional<T> expected, SymbolAliases symbolAliases)
        {
            if (actual.isPresent() != expected.isPresent()) {
                return false;
            }
            if (actual.isPresent()) {
                return process(actual.get(), new Context(expected.get(), symbolAliases));
            }
            return true;
        }
    }

    private static class Context
    {
        private final Node expectedNode;
        private final SymbolAliases symbolAliases;

        Context(Node expectedNode, SymbolAliases symbolAliases)
        {
            this.expectedNode = expectedNode;
            this.symbolAliases = symbolAliases;
        }

        Node getExpectedNode()
        {
            return expectedNode;
        }

        SymbolAliases getSymbolAliases()
        {
            return symbolAliases;
        }

        Context replaceExpectedNode(Expression expected)
        {
            return new Context(expected, symbolAliases);
        }
    }
}
