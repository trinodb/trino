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
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.assertions.ExpressionVerifier;
import io.prestosql.sql.planner.assertions.SymbolAliases;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static org.testng.Assert.assertTrue;

public class TestDesugarLikeRewriter
        extends BaseRuleTest
{
    private static final Map<Symbol, Type> symbols = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("x"), VARCHAR)
            .build();

    private static final TypeProvider TYPE_PROVIDER = TypeProvider.copyOf(symbols);
    private static final SymbolAliases SYMBOL_ALIASES = SymbolAliases.builder().put("x", new SymbolReference("x")).build();

    @Test
    public void testStartsWithOptimization()
    {
        Expression initial = new LikePredicate(
                new SymbolReference("x"),
                new StringLiteral("test%"),
                Optional.empty());

        Expression rewritten = tryRewrite(initial);

        Expression expected = new FunctionCallBuilder(tester().getMetadata())
                .setName(QualifiedName.of("STARTS_WITH"))
                .addArgument(VARCHAR, new SymbolReference("x"))
                .addArgument(VARCHAR, new StringLiteral("test"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(SYMBOL_ALIASES);
        assertTrue(verifier.process(rewritten, expected));
    }

    @Test
    public void testStartsWithOptimizationWithEscape()
    {
        Expression initial = new LikePredicate(
                new SymbolReference("x"),
                new StringLiteral("test\\_test%"),
                Optional.of(new StringLiteral("\\")));

        Expression rewritten = tryRewrite(initial);

        Expression expected = new FunctionCallBuilder(tester().getMetadata())
                .setName(QualifiedName.of("STARTS_WITH"))
                .addArgument(VARCHAR, new SymbolReference("x"))
                .addArgument(VARCHAR, new StringLiteral("test_test"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(SYMBOL_ALIASES);
        assertTrue(verifier.process(rewritten, expected));
    }

    @Test
    public void testEndsWithOptimization()
    {
        Expression initial = new LikePredicate(
                new SymbolReference("x"),
                new StringLiteral("%test"),
                Optional.empty());

        Expression rewritten = tryRewrite(initial);

        Expression expected = new FunctionCallBuilder(tester().getMetadata())
                .setName(QualifiedName.of("ENDS_WITH"))
                .addArgument(VARCHAR, new SymbolReference("x"))
                .addArgument(VARCHAR, new StringLiteral("test"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(SYMBOL_ALIASES);
        assertTrue(verifier.process(rewritten, expected));
    }

    @Test
    public void testEndsWithOptimizationWithEscape()
    {
        Expression initial = new LikePredicate(
                new SymbolReference("x"),
                new StringLiteral("%test\\_test"),
                Optional.of(new StringLiteral("\\")));

        Expression rewritten = tryRewrite(initial);

        Expression expected = new FunctionCallBuilder(tester().getMetadata())
                .setName(QualifiedName.of("ENDS_WITH"))
                .addArgument(VARCHAR, new SymbolReference("x"))
                .addArgument(VARCHAR, new StringLiteral("test_test"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(SYMBOL_ALIASES);
        assertTrue(verifier.process(rewritten, expected));
    }

    @Test
    public void testNotOptimized()
    {
        Expression initial = new LikePredicate(
                new SymbolReference("x"),
                new StringLiteral("test_test%"),
                Optional.empty());

        Expression rewritten = tryRewrite(initial);

        Expression expected = new FunctionCallBuilder(tester().getMetadata())
                .setName(QualifiedName.of("LIKE"))
                .addArgument(VARCHAR, new SymbolReference("x"))
                .addArgument(LIKE_PATTERN, new FunctionCallBuilder(tester().getMetadata())
                        .setName(QualifiedName.of(LIKE_PATTERN_FUNCTION_NAME))
                        .addArgument(VARCHAR, new StringLiteral("test_test%"))
                        .build())
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(SYMBOL_ALIASES);
        assertTrue(verifier.process(rewritten, expected));
    }

    private Expression tryRewrite(Expression expression)
    {
        Expression rewritten = DesugarLikeRewriter.rewrite(
                expression,
                tester().getSession(),
                tester().getMetadata(),
                tester().getTypeAnalyzer(),
                TYPE_PROVIDER);

        return rewritten;
    }
}
