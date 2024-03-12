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
package io.trino.sql.planner.assertions;

import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.WhenClause;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.metadata.ResolvedFunction.isResolved;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
public final class ExpressionVerifier
        extends AstVisitor<Boolean, Expression>
{
    private final SymbolAliases symbolAliases;

    public ExpressionVerifier(SymbolAliases symbolAliases)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
    }

    @Override
    protected Boolean visitNode(Node node, Expression expectedExpression)
    {
        throw new IllegalStateException(format("Node %s is not supported", node.getClass().getSimpleName()));
    }

    @Override
    protected Boolean visitGenericLiteral(GenericLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof GenericLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression)) &&
                actual.getType().equalsIgnoreCase(((GenericLiteral) expectedExpression).getType());
    }

    @Override
    protected Boolean visitStringLiteral(StringLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof StringLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof LongLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof DoubleLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
    }

    @Override
    protected Boolean visitDecimalLiteral(DecimalLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof DecimalLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof BooleanLiteral)) {
            return false;
        }

        return getValueFromLiteral(actual).equals(getValueFromLiteral(expectedExpression));
    }

    @Override
    protected Boolean visitNullLiteral(NullLiteral node, Expression expectedExpression)
    {
        return expectedExpression instanceof NullLiteral;
    }

    private static String getValueFromLiteral(Node expression)
    {
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getParsedValue());
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

        if (expression instanceof StringLiteral) {
            return ((StringLiteral) expression).getValue();
        }

        throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof SymbolReference expected)) {
            return false;
        }

        return symbolAliases.get(expected.getName()).equals(actual);
    }

    @Override
    protected Boolean visitDereferenceExpression(DereferenceExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof DereferenceExpression expected)) {
            return false;
        }

        return actual.getField().equals(expected.getField()) &&
                process(actual.getBase(), expected.getBase());
    }

    @Override
    protected Boolean visitIfExpression(IfExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof IfExpression expected)) {
            return false;
        }

        return process(actual.getCondition(), expected.getCondition())
                && process(actual.getTrueValue(), expected.getTrueValue())
                && process(actual.getFalseValue(), expected.getFalseValue());
    }

    @Override
    protected Boolean visitCast(Cast actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Cast expected)) {
            return false;
        }

        // TODO: hack!! The type in Cast is an AST structure, subject to case-sensitivity and quoting rules
        // Here we're trying to verify its IR counterpart, but the plan testing framework goes directly
        // from SQL text -> IR-like expressions without doing all the proper canonicalizations. So we cheat
        // here and normalize everything to the same case before comparing
        if (!actual.getType().toString().equalsIgnoreCase(expected.getType().toString())) {
            return false;
        }

        return process(actual.getExpression(), expected.getExpression());
    }

    @Override
    protected Boolean visitIsNullPredicate(IsNullPredicate actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof IsNullPredicate expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue());
    }

    @Override
    protected Boolean visitIsNotNullPredicate(IsNotNullPredicate actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof IsNotNullPredicate expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue());
    }

    @Override
    protected Boolean visitInPredicate(InPredicate actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof InPredicate expected)) {
            return false;
        }

        if (actual.getValueList() instanceof InListExpression || !(expected.getValueList() instanceof InListExpression)) {
            return process(actual.getValue(), expected.getValue()) &&
                    process(actual.getValueList(), expected.getValueList());
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
        checkState(values.size() == 1, "Multiple expressions in expected value list %s, but actual value is not a list: %s", values, actual.getValue());
        Expression onlyExpectedExpression = values.get(0);
        return process(actual.getValue(), expected.getValue()) &&
                process(actual.getValueList(), onlyExpectedExpression);
    }

    @Override
    protected Boolean visitInListExpression(InListExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof InListExpression expected)) {
            return false;
        }

        return process(actual.getValues(), expected.getValues());
    }

    @Override
    protected Boolean visitComparisonExpression(ComparisonExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof ComparisonExpression expected)) {
            return false;
        }

        if (actual.getOperator() == expected.getOperator() &&
                process(actual.getLeft(), expected.getLeft()) &&
                process(actual.getRight(), expected.getRight())) {
            return true;
        }

        return actual.getOperator() == expected.getOperator().flip() &&
                process(actual.getLeft(), expected.getRight()) &&
                process(actual.getRight(), expected.getLeft());
    }

    @Override
    protected Boolean visitBetweenPredicate(BetweenPredicate actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof BetweenPredicate expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue()) &&
                process(actual.getMin(), expected.getMin()) &&
                process(actual.getMax(), expected.getMax());
    }

    @Override
    protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof ArithmeticUnaryExpression expected)) {
            return false;
        }

        return actual.getSign() == expected.getSign() &&
                process(actual.getValue(), expected.getValue());
    }

    @Override
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof ArithmeticBinaryExpression expected)) {
            return false;
        }

        return actual.getOperator() == expected.getOperator() &&
                process(actual.getLeft(), expected.getLeft()) &&
                process(actual.getRight(), expected.getRight());
    }

    @Override
    protected Boolean visitNotExpression(NotExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof NotExpression expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue());
    }

    @Override
    protected Boolean visitLogicalExpression(LogicalExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof LogicalExpression expected)) {
            return false;
        }

        if (actual.getTerms().size() != expected.getTerms().size() || actual.getOperator() != expected.getOperator()) {
            return false;
        }

        for (int i = 0; i < actual.getTerms().size(); i++) {
            if (!process(actual.getTerms().get(i), expected.getTerms().get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof CoalesceExpression expected)) {
            return false;
        }

        if (actual.getOperands().size() != expected.getOperands().size()) {
            return false;
        }

        for (int i = 0; i < actual.getOperands().size(); i++) {
            if (!process(actual.getOperands().get(i), expected.getOperands().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Boolean visitSimpleCaseExpression(SimpleCaseExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof SimpleCaseExpression expected)) {
            return false;
        }

        return process(actual.getOperand(), expected.getOperand()) &&
                process(actual.getWhenClauses(), expected.getWhenClauses()) &&
                process(actual.getDefaultValue(), expected.getDefaultValue());
    }

    @Override
    protected Boolean visitSearchedCaseExpression(SearchedCaseExpression actual, Expression expected)
    {
        if (!(expected instanceof SearchedCaseExpression expectedCase)) {
            return false;
        }

        if (!process(actual.getWhenClauses(), expectedCase.getWhenClauses())) {
            return false;
        }

        if (actual.getDefaultValue().isPresent() != expectedCase.getDefaultValue().isPresent()) {
            return false;
        }

        return process(actual.getDefaultValue(), expectedCase.getDefaultValue());
    }

    @Override
    protected Boolean visitWhenClause(WhenClause actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof WhenClause expected)) {
            return false;
        }

        return process(actual.getOperand(), expected.getOperand()) &&
                process(actual.getResult(), expected.getResult());
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof FunctionCall expected)) {
            return false;
        }

        CatalogSchemaFunctionName expectedFunctionName;
        if (isResolved(expected.getName())) {
            expectedFunctionName = extractFunctionName(expected.getName());
        }
        else {
            checkArgument(expected.getName().getParts().size() == 1, "Unresolved function call name must not be qualified: %s", expected.getName());
            expectedFunctionName = builtinFunctionName(expected.getName().getSuffix());
        }

        return extractFunctionName(actual.getName()).equals(expectedFunctionName) &&
                process(actual.getArguments(), expected.getArguments());
    }

    @Override
    protected Boolean visitLambdaExpression(LambdaExpression actual, Expression expected)
    {
        if (!(expected instanceof LambdaExpression lambdaExpression)) {
            return false;
        }

        // todo this should allow the arguments to have different names
        if (!actual.getArguments().equals(lambdaExpression.getArguments())) {
            return false;
        }

        return process(actual.getBody(), lambdaExpression.getBody());
    }

    @Override
    protected Boolean visitRow(Row actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Row expected)) {
            return false;
        }

        return process(actual.getItems(), expected.getItems());
    }

    @Override
    protected Boolean visitTryExpression(TryExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof TryExpression expected)) {
            return false;
        }

        return process(actual.getInnerExpression(), expected.getInnerExpression());
    }

    @Override
    protected Boolean visitSubscriptExpression(SubscriptExpression actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof SubscriptExpression expected)) {
            return false;
        }

        return process(actual.getBase(), expected.getBase()) && process(actual.getIndex(), expected.getIndex());
    }

    private <T extends Expression> boolean process(List<T> actuals, List<T> expecteds)
    {
        if (actuals.size() != expecteds.size()) {
            return false;
        }
        for (int i = 0; i < actuals.size(); i++) {
            if (!process(actuals.get(i), expecteds.get(i))) {
                return false;
            }
        }
        return true;
    }

    private <T extends Expression> boolean process(Optional<T> actual, Optional<T> expected)
    {
        if (actual.isPresent() != expected.isPresent()) {
            return false;
        }
        if (actual.isPresent()) {
            return process(actual.get(), expected.get());
        }
        return true;
    }
}
