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
package io.trino.util;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.LogicalPlanner.noTruncationCast;
import static java.util.Locale.ENGLISH;

public final class ColumnDefaultOptions
{
    private ColumnDefaultOptions() {}

    public static ConnectorExpression evaluateDefaultValue(
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            Type columnType,
            Expression defaultLiteral)
    {
        if (!(defaultLiteral instanceof Literal literal)) {
            throw new IllegalArgumentException("Unsupported default expression: " + defaultLiteral);
        }

        try {
            ExpressionAnalyzer constantAnalyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, warningCollector);
            Type literalType = constantAnalyzer.analyze(literal, Scope.create());
            Object value = evaluateConstant(literal, literalType, plannerContext, session, accessControl);

            if (!literalType.equals(columnType)) {
                value = new IrExpressionEvaluator(plannerContext).evaluate(
                        noTruncationCast(plannerContext.getMetadata(), new io.trino.sql.ir.Constant(literalType, value), literalType, columnType),
                        session,
                        ImmutableMap.of());
            }

            return new Constant(value, columnType);
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_LITERAL, literal, e, "'%s' is not a valid %s literal", literal, columnType.getDisplayName().toUpperCase(ENGLISH));
        }
    }
}
