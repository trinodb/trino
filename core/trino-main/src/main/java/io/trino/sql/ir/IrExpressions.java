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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.type.TypeCoercion;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.isDynamicFilterFunction;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;

public class IrExpressions
{
    private IrExpressions() {}

    public static Expression ifExpression(Expression condition, Expression trueCase)
    {
        return new Case(ImmutableList.of(new WhenClause(condition, trueCase)), new Constant(trueCase.type(), null));
    }

    public static Expression ifExpression(Expression condition, Expression trueCase, Expression falseCase)
    {
        return new Case(ImmutableList.of(new WhenClause(condition, trueCase)), falseCase);
    }

    public static boolean mayFail(PlannerContext plannerContext, Expression expression)
    {
        return switch (expression) {
            case Array e -> e.elements().stream().anyMatch(element -> mayFail(plannerContext, element));
            case Between e -> mayFail(plannerContext, e.value()) || mayFail(plannerContext, e.min()) || mayFail(plannerContext, e.max());
            case Bind e -> false;
            case Call e -> mayFail(e.function()) || e.arguments().stream().anyMatch(argument -> mayFail(plannerContext, argument)); // TODO: allow functions to be marked as non-failing
            case Case e -> e.whenClauses().stream().anyMatch(clause -> mayFail(plannerContext, clause.getOperand()) || mayFail(plannerContext, clause.getResult())) ||
                    mayFail(plannerContext, e.defaultValue());
            case Cast e -> mayFail(plannerContext, e);
            case Coalesce e -> e.operands().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case Comparison e -> mayFail(plannerContext, e.left()) || mayFail(plannerContext, e.right());
            case Constant e -> false;
            case FieldReference e -> false;
            case In e -> mayFail(plannerContext, e.value()) || e.valueList().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case IsNull e -> mayFail(plannerContext, e.value());
            case Lambda e -> false;
            case Logical e -> e.terms().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case Not e -> mayFail(plannerContext, e.value());
            case NullIf e -> mayFail(plannerContext, e.first()) || mayFail(plannerContext, e.second());
            case Reference e -> false;
            case Row e -> e.items().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case Switch e -> mayFail(plannerContext, e.operand()) || e.whenClauses().stream().anyMatch(clause -> mayFail(plannerContext, clause.getOperand()) || mayFail(plannerContext, clause.getResult())) ||
                    mayFail(plannerContext, e.defaultValue());
        };
    }

    // TODO: record "safety" (can the cast fail at runtime) in Cast node (separate from "may return null" that's currently implied by try_cast)
    private static boolean mayFail(PlannerContext plannerContext, Cast cast)
    {
        if (cast.safe()) {
            return false;
        }

        TypeCoercion coercions = new TypeCoercion(plannerContext.getTypeManager()::getType);
        if (coercions.canCoerce(cast.expression().type(), cast.type())) {
            return false;
        }

        if (cast.type().equals(VARCHAR)) {
            return false;
        }

        return true;
    }

    private static boolean mayFail(ResolvedFunction function)
    {
        // TODO: these should be attributes of the function
        CatalogSchemaFunctionName name = function.name();
        return !name.equals(builtinFunctionName("length")) &&
                !name.equals(builtinFunctionName("substring")) &&
                !name.equals(builtinFunctionName(LIKE_FUNCTION_NAME)) &&
                !isDynamicFilterFunction(function.name());
    }
}
