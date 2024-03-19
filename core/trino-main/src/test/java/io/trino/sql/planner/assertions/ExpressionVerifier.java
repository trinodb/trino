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

import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ArithmeticNegation;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.CoalesceExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.InPredicate;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.LambdaExpression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.SimpleCaseExpression;
import io.trino.sql.ir.SubscriptExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.ir.WhenClause;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.readNativeValue;
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
        extends IrVisitor<Boolean, Expression>
{
    private final SymbolAliases symbolAliases;

    public ExpressionVerifier(SymbolAliases symbolAliases)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
    }

    @Override
    protected Boolean visitConstant(Constant actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Constant expected)) {
            return false;
        }

        if (!actual.getType().equals(expected.getType())) {
            return false;
        }

        if (actual.getType() instanceof RowType type && rowsEqual(type, (SqlRow) expected.getValue(), (SqlRow) actual.getValue())) {
            return false;
        }

        return Objects.equals(actual.getValue(), expected.getValue());
    }

    /**
     * Compare two rows for representational equality
     */
    public static boolean rowsEqual(RowType type, SqlRow expectedRow, SqlRow actualRow)
    {
        for (int i = 0; i < type.getFields().size(); i++) {
            Type fieldType = type.getFields().get(i).getType();
            Object expectedField = readNativeValue(fieldType, expectedRow.getRawFieldBlock(i), expectedRow.getRawIndex());
            Object actualField = readNativeValue(fieldType, actualRow.getRawFieldBlock(i), actualRow.getRawIndex());
            if (!Objects.equals(expectedField, actualField)) {
                return false;
            }
        }
        return true;
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
    protected Boolean visitInPredicate(InPredicate actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof InPredicate expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue()) &&
                process(actual.getValueList(), expected.getValueList());
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
    protected Boolean visitArithmeticNegation(ArithmeticNegation actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof ArithmeticNegation expected)) {
            return false;
        }

        return process(actual.getValue(), expected.getValue());
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
                processWhenClauses(actual.getWhenClauses(), expected.getWhenClauses()) &&
                process(actual.getDefaultValue(), expected.getDefaultValue());
    }

    @Override
    protected Boolean visitSearchedCaseExpression(SearchedCaseExpression actual, Expression expected)
    {
        if (!(expected instanceof SearchedCaseExpression expectedCase)) {
            return false;
        }

        if (!processWhenClauses(actual.getWhenClauses(), expectedCase.getWhenClauses())) {
            return false;
        }

        if (actual.getDefaultValue().isPresent() != expectedCase.getDefaultValue().isPresent()) {
            return false;
        }

        return process(actual.getDefaultValue(), expectedCase.getDefaultValue());
    }

    private boolean processWhenClauses(List<WhenClause> actual, List<WhenClause> expected)
    {
        if (actual.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < actual.size(); i++) {
            if (!process(actual.get(i), expected.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean process(WhenClause actual, WhenClause expected)
    {
        return process(actual.getOperand(), expected.getOperand()) &&
                process(actual.getResult(), expected.getResult());
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof FunctionCall expected)) {
            return false;
        }

        return actual.getFunction().getName().equals(expected.getFunction().getName()) &&
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
