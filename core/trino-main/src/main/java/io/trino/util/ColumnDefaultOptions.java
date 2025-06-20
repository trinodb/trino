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
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import jakarta.annotation.Nullable;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.LogicalPlanner.noTruncationCast;
import static java.util.Locale.ENGLISH;

public final class ColumnDefaultOptions
{
    private ColumnDefaultOptions() {}

    @Nullable
    public static String evaluateDefaultValue(
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            Type columnType,
            String defaultLiteralExpression)
    {
        Expression defaultLiteral = new SqlParser().createExpression(defaultLiteralExpression);

        if (!(defaultLiteral instanceof Literal literal)) {
            throw new IllegalArgumentException("Unsupported default expression: " + defaultLiteral);
        }

        try {
            ExpressionAnalyzer constantAnalyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, warningCollector);
            Type literalType = constantAnalyzer.analyze(literal, Scope.create());
            Object value = evaluateConstant(literal, literalType, plannerContext, session, accessControl);
            if (value == null) {
                return null;
            }

            IrExpressionEvaluator expressionEvaluator = new IrExpressionEvaluator(plannerContext);
            if (!literalType.equals(columnType)) {
                value = expressionEvaluator.evaluate(
                        noTruncationCast(plannerContext.getMetadata(), new io.trino.sql.ir.Constant(literalType, value), literalType, columnType),
                        session,
                        ImmutableMap.of());
            }

            Type castType = VARCHAR;
            if (columnType instanceof VarcharType || columnType instanceof CharType) {
                castType = columnType;
            }

            // TODO Find the correct way to cast default values to String
            Slice slice = (Slice) expressionEvaluator.evaluate(
                    new Cast(new io.trino.sql.ir.Constant(columnType, value), castType),
                    session,
                    ImmutableMap.of());
            return slice.toStringUtf8();
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_LITERAL, literal, e, "'%s' is not a valid %s literal", literal, columnType.getDisplayName().toUpperCase(ENGLISH));
        }
    }
}
