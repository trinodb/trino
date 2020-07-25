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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public final class DesugarLikeRewriter
{
    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes, metadata), expression);
    }

    private DesugarLikeRewriter() {}

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider typeProvider)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, typeProvider, expression);

        return rewrite(expression, expressionTypes, metadata);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;
        private final Metadata metadata;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes, Metadata metadata)
        {
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
            this.metadata = metadata;
        }

        @Override
        public FunctionCall rewriteLikePredicate(LikePredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            LikePredicate rewritten = treeRewriter.defaultRewrite(node, context);

            Optional<FunctionCall> startsWithFunctionCall = tryRewriteToStartsWith(node, rewritten);
            if (startsWithFunctionCall.isPresent()) {
                return startsWithFunctionCall.get();
            }

            Optional<FunctionCall> endsWithFunctionCall = tryRewriteToEndsWith(node, rewritten);
            if (endsWithFunctionCall.isPresent()) {
                return endsWithFunctionCall.get();
            }

            FunctionCall patternCall;
            if (rewritten.getEscape().isPresent()) {
                patternCall = new FunctionCallBuilder(metadata)
                        .setName(QualifiedName.of(LIKE_PATTERN_FUNCTION_NAME))
                        .addArgument(getType(node.getPattern()), rewritten.getPattern())
                        .addArgument(getType(node.getEscape().get()), rewritten.getEscape().get())
                        .build();
            }
            else {
                patternCall = new FunctionCallBuilder(metadata)
                        .setName(QualifiedName.of(LIKE_PATTERN_FUNCTION_NAME))
                        .addArgument(VARCHAR, rewritten.getPattern())
                        .build();
            }

            return new FunctionCallBuilder(metadata)
                    .setName(QualifiedName.of("LIKE"))
                    .addArgument(getType(node.getValue()), rewritten.getValue())
                    .addArgument(LIKE_PATTERN, patternCall)
                    .build();
        }

        private Type getType(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }

        private Optional<FunctionCall> tryRewriteToStartsWith(LikePredicate node, LikePredicate rewritten)
        {
            if (canRewrite(rewritten)) {
                String patternString = ((StringLiteral) rewritten.getPattern()).getValue();
                char escapeChar = (char) -1;
                if (rewritten.getEscape().isPresent()) {
                    escapeChar = getEscapeCharacter(((StringLiteral) rewritten.getEscape().get()).getValue());
                }

                Optional<String> prefix = getSearchPrefix(patternString, escapeChar);
                if (prefix.isPresent()) {
                    FunctionCall functionCall = new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("STARTS_WITH"))
                            .addArgument(getType(node.getValue()), rewritten.getValue())
                            .addArgument(VARCHAR, new StringLiteral(prefix.get()))
                            .build();
                    return Optional.of(functionCall);
                }
            }
            return Optional.empty();
        }

        private Optional<FunctionCall> tryRewriteToEndsWith(LikePredicate node, LikePredicate rewritten)
        {
            if (canRewrite(rewritten)) {
                String patternString = ((StringLiteral) rewritten.getPattern()).getValue();
                char escapeChar = (char) -1;
                if (rewritten.getEscape().isPresent()) {
                    escapeChar = getEscapeCharacter(((StringLiteral) rewritten.getEscape().get()).getValue());
                }

                Optional<String> suffix = getSearchSuffix(patternString, escapeChar);
                if (suffix.isPresent()) {
                    FunctionCall functionCall = new FunctionCallBuilder(metadata)
                            .setName(QualifiedName.of("ENDS_WITH"))
                            .addArgument(getType(node.getValue()), rewritten.getValue())
                            .addArgument(VARCHAR, new StringLiteral(suffix.get()))
                            .build();
                    return Optional.of(functionCall);
                }
            }
            return Optional.empty();
        }

        private static boolean canRewrite(LikePredicate rewritten)
        {
            return rewritten.getPattern() instanceof StringLiteral && (rewritten.getEscape().isEmpty() || rewritten.getEscape().get() instanceof StringLiteral);
        }

        private static char getEscapeCharacter(String stringEscape)
        {
            if (stringEscape.isEmpty()) {
                return (char) -1;
            }
            checkCondition(stringEscape.length() == 1, INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
            return stringEscape.charAt(0);
        }

        private static Optional<String> getSearchPrefix(String patternString, char escapeChar)
        {
            int len = patternString.length();
            if (len >= 2 && patternString.charAt(len - 1) == '%' && (escapeChar == -1 || patternString.charAt(len - 2) != escapeChar)) {
                return convertToSearchString(patternString.substring(0, len - 1), escapeChar);
            }
            return Optional.empty();
        }

        private static Optional<String> getSearchSuffix(String patternString, char escapeChar)
        {
            int len = patternString.length();
            if (len >= 2 && patternString.charAt(0) == '%') {
                return convertToSearchString(patternString.substring(1, len), escapeChar);
            }
            return Optional.empty();
        }

        private static Optional<String> convertToSearchString(String pattern, char escapeChar)
        {
            boolean escaped = false;
            StringBuilder sb = new StringBuilder();
            for (char currentChar : pattern.toCharArray()) {
                checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
                if (!escaped && currentChar == escapeChar) {
                    escaped = true;
                }
                else {
                    switch (currentChar) {
                        case '%':
                        case '_':
                            if (!escaped) {
                                return Optional.empty();
                            }
                        default:
                    }
                    sb.append(currentChar);
                    escaped = false;
                }
            }
            return Optional.of(sb.toString());
        }

        private static void checkEscape(boolean condition)
        {
            checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
        }
    }
}
