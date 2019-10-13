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
package io.prestosql.sql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.assertions.ExpressionVerifier;
import io.prestosql.sql.planner.assertions.SymbolAliases;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.prestosql.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static org.testng.internal.EclipseInterface.ASSERT_LEFT;
import static org.testng.internal.EclipseInterface.ASSERT_MIDDLE;
import static org.testng.internal.EclipseInterface.ASSERT_RIGHT;

public final class ExpressionTestUtils
{
    private ExpressionTestUtils() {}

    public static QualifiedName getFunctionName(FunctionCall actual)
    {
        return ResolvedFunction.fromQualifiedName(actual.getName())
                .map(ResolvedFunction::getSignature)
                .map(Signature::getName)
                .map(QualifiedName::of)
                .orElse(actual.getName());
    }

    public static void assertExpressionEquals(Expression actual, Expression expected)
    {
        assertExpressionEquals(actual, expected, new SymbolAliases());
    }

    public static void assertExpressionEquals(Expression actual, Expression expected, SymbolAliases symbolAliases)
    {
        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        if (!verifier.process(actual, expected)) {
            failNotEqual(actual, expected, null);
        }
    }

    private static void failNotEqual(Object actual, Object expected, String message)
    {
        String formatted = "";
        if (message != null) {
            formatted = message + " ";
        }
        throw new AssertionError(formatted + ASSERT_LEFT + expected + ASSERT_MIDDLE + actual + ASSERT_RIGHT);
    }

    public static Expression planExpression(Metadata metadata, Session session, TypeProvider typeProvider, Expression expression)
    {
        expression = rewriteIdentifiersToSymbolReferences(expression);

        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata,
                session,
                typeProvider,
                ImmutableMap.of(),
                node -> semanticException(EXPRESSION_NOT_CONSTANT, node, "Constant expression cannot contain a subquery"),
                WarningCollector.NOOP,
                false);
        analyzer.analyze(expression, Scope.builder().build());

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                ResolvedFunction resolvedFunction = analyzer.getResolvedFunctions().get(NodeRef.of(node));
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
                FunctionCall newFunctionCall = new FunctionCall(
                        rewritten.getLocation(),
                        resolvedFunction.toQualifiedName(),
                        rewritten.getWindow(),
                        rewritten.getFilter(),
                        rewritten.getOrderBy(),
                        rewritten.isDistinct(),
                        rewritten.getNullTreatment(),
                        rewritten.getArguments());
                return coerceIfNecessary(node, newFunctionCall);
            }

            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                rewrittenExpression = coerceIfNecessary(node, rewrittenExpression);

                return rewrittenExpression;
            }

            private Expression coerceIfNecessary(Expression originalExpression, Expression rewrittenExpression)
            {
                // cast expression if coercion is registered
                Type coercion = analyzer.getExpressionCoercions().get(NodeRef.of(originalExpression));
                if (coercion != null) {
                    rewrittenExpression = new Cast(
                            rewrittenExpression,
                            toSqlType(coercion),
                            false,
                            analyzer.getTypeOnlyCoercions().contains(NodeRef.of(originalExpression)));
                }
                return rewrittenExpression;
            }
        }, expression);
    }
}
