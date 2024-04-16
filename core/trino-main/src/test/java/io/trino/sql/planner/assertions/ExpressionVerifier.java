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

import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;

import java.util.List;
import java.util.Objects;

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
    protected Boolean visitArray(Array actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Array expected)) {
            return false;
        }

        return process(actual.elements(), expected.elements());
    }

    @Override
    protected Boolean visitConstant(Constant actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Constant expected)) {
            return false;
        }

        return Objects.equals(actual.value(), expected.value()) &&
                actual.type().equals(expected.type());
    }

    @Override
    protected Boolean visitReference(Reference actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Reference expected)) {
            return false;
        }

        // TODO: verify types. This is currently hard to do because planner tests
        //       are either missing types, have the wrong types, or they are unable to
        //       provide types due to limitations in the matcher infrastructure
        return symbolAliases.get(expected.name()).name().equals(actual.name());
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
        if (!actual.type().toString().equalsIgnoreCase(expected.type().toString())) {
            return false;
        }

        return process(actual.expression(), expected.expression());
    }

    @Override
    protected Boolean visitIsNull(IsNull actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof IsNull expected)) {
            return false;
        }

        return process(actual.value(), expected.value());
    }

    @Override
    protected Boolean visitIn(In actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof In expected)) {
            return false;
        }

        return process(actual.value(), expected.value()) &&
                process(actual.valueList(), expected.valueList());
    }

    @Override
    protected Boolean visitComparison(Comparison actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Comparison expected)) {
            return false;
        }

        if (actual.operator() == expected.operator() &&
                process(actual.left(), expected.left()) &&
                process(actual.right(), expected.right())) {
            return true;
        }

        return actual.operator() == expected.operator().flip() &&
                process(actual.left(), expected.right()) &&
                process(actual.right(), expected.left());
    }

    @Override
    protected Boolean visitBetween(Between actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Between expected)) {
            return false;
        }

        return process(actual.value(), expected.value()) &&
                process(actual.min(), expected.min()) &&
                process(actual.max(), expected.max());
    }

    @Override
    protected Boolean visitNot(Not actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Not expected)) {
            return false;
        }

        return process(actual.value(), expected.value());
    }

    @Override
    protected Boolean visitLogical(Logical actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Logical expected)) {
            return false;
        }

        if (actual.terms().size() != expected.terms().size() || actual.operator() != expected.operator()) {
            return false;
        }

        for (int i = 0; i < actual.terms().size(); i++) {
            if (!process(actual.terms().get(i), expected.terms().get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected Boolean visitCoalesce(Coalesce actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Coalesce expected)) {
            return false;
        }

        if (actual.operands().size() != expected.operands().size()) {
            return false;
        }

        for (int i = 0; i < actual.operands().size(); i++) {
            if (!process(actual.operands().get(i), expected.operands().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Boolean visitSwitch(Switch actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Switch expected)) {
            return false;
        }

        return process(actual.operand(), expected.operand()) &&
                processWhenClauses(actual.whenClauses(), expected.whenClauses()) &&
                process(actual.defaultValue(), expected.defaultValue());
    }

    @Override
    protected Boolean visitCase(Case actual, Expression expected)
    {
        if (!(expected instanceof Case expectedCase)) {
            return false;
        }

        if (!processWhenClauses(actual.whenClauses(), expectedCase.whenClauses())) {
            return false;
        }

        return process(actual.defaultValue(), expectedCase.defaultValue());
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
    protected Boolean visitCall(Call actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Call expected)) {
            return false;
        }

        return actual.function().name().equals(expected.function().name()) &&
                process(actual.arguments(), expected.arguments());
    }

    @Override
    protected Boolean visitLambda(Lambda actual, Expression expected)
    {
        if (!(expected instanceof Lambda lambda)) {
            return false;
        }

        // todo this should allow the arguments to have different names
        if (!actual.arguments().equals(lambda.arguments())) {
            return false;
        }

        return process(actual.body(), lambda.body());
    }

    @Override
    protected Boolean visitRow(Row actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Row expected)) {
            return false;
        }

        return process(actual.items(), expected.items());
    }

    @Override
    protected Boolean visitFieldReference(FieldReference actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof FieldReference expected)) {
            return false;
        }

        return process(actual.base(), expected.base()) && actual.field() == expected.field();
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
}
