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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.TranslationMap;
import io.trino.sql.tree.Expression;
import io.trino.type.TypeCoercion;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class ConstantEvaluator
{
    private ConstantEvaluator() {}

    public static Object evaluateConstant(
            Expression expression,
            Type expectedType,
            PlannerContext plannerContext,
            Session session,
            AccessControl accessControl)
    {
        Analysis analysis = new Analysis(null, ImmutableMap.of(), QueryType.OTHERS);
        Scope scope = Scope.create();
        ExpressionAnalyzer.analyzeExpressionWithoutSubqueries(
                session,
                plannerContext,
                accessControl,
                scope,
                analysis,
                expression,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                WarningCollector.NOOP,
                CorrelationSupport.DISALLOWED);

        TranslationMap translationMap = new TranslationMap(Optional.empty(), scope, analysis, ImmutableMap.of(), ImmutableList.of(), session, plannerContext);
        io.trino.sql.ir.Expression rewritten = translationMap.rewrite(expression);

        Type actualType = rewritten.type();
        if (!new TypeCoercion(plannerContext.getTypeManager()::getType).canCoerce(actualType, expectedType)) {
            throw semanticException(TYPE_MISMATCH, expression, "Cannot cast type %s to %s", actualType.getDisplayName(), expectedType.getDisplayName());
        }

        if (!actualType.equals(expectedType)) {
            rewritten = new Cast(rewritten, expectedType, false);
        }

        return new IrExpressionInterpreter(rewritten, plannerContext, session).evaluate();
    }
}
