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
package io.trino.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.DesugarArrayConstructorRewriter;
import io.trino.sql.planner.DesugarLikeRewriter;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.ExpressionVerifier;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;
import io.trino.transaction.TestingTransactionManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.testng.internal.EclipseInterface.ASSERT_LEFT;
import static org.testng.internal.EclipseInterface.ASSERT_MIDDLE;
import static org.testng.internal.EclipseInterface.ASSERT_RIGHT;

public final class ExpressionTestUtils
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private ExpressionTestUtils() {}

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

    public static Expression createExpression(Session session, String expression, Metadata metadata, TypeProvider symbolTypes)
    {
        Expression parsedExpression = SQL_PARSER.createExpression(expression, createParsingOptions(session));
        return planExpression(metadata, session, symbolTypes, parsedExpression);
    }

    public static Expression createExpression(String expression, Metadata metadata, TypeProvider symbolTypes)
    {
        return createExpression(TEST_SESSION, expression, metadata, symbolTypes);
    }

    public static Expression planExpression(Metadata metadata, Session session, TypeProvider typeProvider, Expression expression)
    {
        return transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Expression rewritten = rewriteIdentifiersToSymbolReferences(expression);
                    rewritten = DesugarLikeRewriter.rewrite(rewritten, transactionSession, metadata, new TypeAnalyzer(SQL_PARSER, metadata), typeProvider);
                    rewritten = DesugarArrayConstructorRewriter.rewrite(rewritten, transactionSession, metadata, new TypeAnalyzer(SQL_PARSER, metadata), typeProvider);
                    rewritten = CanonicalizeExpressionRewriter.rewrite(rewritten, transactionSession, metadata, new TypeAnalyzer(SQL_PARSER, metadata), typeProvider);
                    return resolveFunctionCalls(metadata, transactionSession, typeProvider, rewritten);
                });
    }

    public static Expression resolveFunctionCalls(Metadata metadata, Session session, TypeProvider typeProvider, Expression expression)
    {
        return resolveFunctionCalls(metadata, session, typeProvider, expression, Scope.builder().build());
    }

    public static Expression resolveFunctionCalls(Metadata metadata, Session session, TypeProvider typeProvider, Expression expression, Scope scope)
    {
        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata,
                new AllowAllAccessControl(),
                session,
                typeProvider,
                ImmutableMap.of(),
                node -> semanticException(EXPRESSION_NOT_CONSTANT, node, "Constant expression cannot contain a subquery"),
                WarningCollector.NOOP,
                false);
        analyzer.analyze(expression, scope);

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
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
                        rewritten.getProcessingMode(),
                        rewritten.getArguments());
                return coerceIfNecessary(node, newFunctionCall);
            }

            @Override
            protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
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

    public static Map<NodeRef<Expression>, Type> getTypes(Session session, Metadata metadata, TypeProvider typeProvider, Expression expression)
    {
        return transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    return new TypeAnalyzer(SQL_PARSER, metadata).getTypes(transactionSession, typeProvider, expression);
                });
    }
}
