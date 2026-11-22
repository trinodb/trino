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
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;

class TestExpressionTreeRewriter
{
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    void testInAndBindChildListIdentityAndOrder(int changedIndex)
    {
        List<Expression> references = references();
        Reference replacement = new Reference(BIGINT, changedIndex < 0 ? "unused" : "s" + changedIndex);
        Lambda function = new Lambda(references.stream().map(Symbol::from).toList(), new Constant(BIGINT, 0L));
        for (Expression original : ImmutableList.of(new In(new Constant(BIGINT, 0L), references), new Bind(references, function), new Row(references))) {
            List<Expression> visited = new ArrayList<>();
            Expression result = ExpressionTreeRewriter.rewriteWith(replaceReference(references, changedIndex, replacement, visited), original);
            assertThat(visited).containsExactlyElementsOf(references);
            assertThat(result).isEqualTo(original);
            if (changedIndex < 0) {
                assertThat(result).isSameAs(original);
            }
            else {
                assertThat(result).isNotSameAs(original);
            }
            List<Expression> children = switch (result) {
                case In in -> in.valueList();
                case Bind bind -> bind.values();
                case Row row -> row.items();
                default -> throw new AssertionError("Unexpected expression: " + result);
            };
            for (int index = 0; index < references.size(); index++) {
                assertThat(children.get(index)).isSameAs(index == changedIndex ? replacement : references.get(index));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    void testCaseClauseIdentityAndOrder(int changedIndex)
    {
        List<Expression> references = references();
        List<WhenClause> clauses = references.stream().map(reference -> new WhenClause(TRUE, reference)).toList();
        Case original = new Case(clauses, new Constant(BIGINT, 0L));
        Reference replacement = new Reference(BIGINT, changedIndex < 0 ? "unused" : "s" + changedIndex);
        List<Expression> visited = new ArrayList<>();
        Case result = ExpressionTreeRewriter.rewriteWith(replaceReference(references, changedIndex, replacement, visited), original);

        assertThat(visited).containsExactlyElementsOf(references);
        assertThat(result).isEqualTo(original);
        if (changedIndex < 0) {
            assertThat(result).isSameAs(original);
        }
        for (int index = 0; index < clauses.size(); index++) {
            if (index == changedIndex) {
                assertThat(result.whenClauses().get(index)).isNotSameAs(clauses.get(index));
                assertThat(result.whenClauses().get(index).result()).isSameAs(replacement);
            }
            else {
                assertThat(result.whenClauses().get(index)).isSameAs(clauses.get(index));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    void testMatchClauseIdentityAndOrder(int changedIndex)
    {
        List<Expression> references = references();
        Lambda predicate = new Lambda(ImmutableList.of(new Symbol(BIGINT, "bound")), TRUE);
        List<MatchClause> clauses = references.stream().map(reference -> new MatchClause(predicate, reference)).toList();
        Match original = new Match(new Constant(BIGINT, 1L), clauses, new Constant(BIGINT, 0L));
        Reference replacement = new Reference(BIGINT, changedIndex < 0 ? "unused" : "s" + changedIndex);
        List<Expression> visited = new ArrayList<>();
        Match result = ExpressionTreeRewriter.rewriteWith(replaceReference(references, changedIndex, replacement, visited), original);

        assertThat(visited).containsExactlyElementsOf(references);
        assertThat(result).isEqualTo(original);
        if (changedIndex < 0) {
            assertThat(result).isSameAs(original);
        }
        for (int index = 0; index < clauses.size(); index++) {
            if (index == changedIndex) {
                assertThat(result.clauses().get(index)).isNotSameAs(clauses.get(index));
                assertThat(result.clauses().get(index).result()).isSameAs(replacement);
            }
            else {
                assertThat(result.clauses().get(index)).isSameAs(clauses.get(index));
            }
        }
    }

    @Test
    void testScalarChildrenWithUnchangedClauses()
    {
        Reference input = new Reference(BIGINT, "input");
        Reference replacement = new Reference(BIGINT, "input");
        ExpressionRewriter<Void> rewriter = replaceReference(ImmutableList.of(input), 0, replacement, new ArrayList<>());
        Case originalCase = new Case(ImmutableList.of(new WhenClause(TRUE, new Constant(BIGINT, 1L))), input);
        Case rewrittenCase = ExpressionTreeRewriter.rewriteWith(rewriter, originalCase);
        assertThat(rewrittenCase).isNotSameAs(originalCase);
        assertThat(rewrittenCase.defaultValue()).isSameAs(replacement);
        assertThat(rewrittenCase.whenClauses()).isSameAs(originalCase.whenClauses());

        Lambda predicate = new Lambda(ImmutableList.of(new Symbol(BIGINT, "bound")), TRUE);
        Match originalMatch = new Match(input, ImmutableList.of(new MatchClause(predicate, new Constant(BIGINT, 1L))), input);
        Match rewrittenMatch = ExpressionTreeRewriter.rewriteWith(rewriter, originalMatch);
        assertThat(rewrittenMatch).isNotSameAs(originalMatch);
        assertThat(rewrittenMatch.operand()).isSameAs(replacement);
        assertThat(rewrittenMatch.defaultValue()).isSameAs(replacement);
        assertThat(rewrittenMatch.clauses()).isSameAs(originalMatch.clauses());
    }

    @Test
    void testDefaultRewriteAndNestedContext()
    {
        List<Expression> references = references();
        Row nested = new Row(references.subList(0, 2));
        Row root = new Row(ImmutableList.of(nested, references.get(2)));
        Object outerContext = new Object();
        Object innerContext = new Object();
        List<Row> visitedRows = new ArrayList<>();
        List<Object> contexts = new ArrayList<>();
        ExpressionTreeRewriter<Object> tree = new ExpressionTreeRewriter<>(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteRow(Row node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
            {
                visitedRows.add(node);
                assertThat(context).isSameAs(outerContext);
                return treeRewriter.defaultRewrite(node, innerContext);
            }

            @Override
            public Expression rewriteReference(Reference node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
            {
                contexts.add(context);
                return null;
            }
        });

        assertThat(tree.defaultRewrite(root, outerContext)).isSameAs(root);
        assertThat(visitedRows).containsExactly(nested);
        assertThat(contexts).containsExactly(innerContext, innerContext, outerContext);
    }

    @Test
    void testReentrantRewriteContext()
    {
        Reference first = new Reference(BIGINT, "first");
        Reference second = new Reference(BIGINT, "second");
        Reference replacement = new Reference(BIGINT, "replacement");
        Object outerContext = new Object();
        Object innerContext = new Object();
        List<Object> contexts = new ArrayList<>();
        ExpressionRewriter<Object> rewriter = new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
            {
                contexts.add(context);
                if (node == first) {
                    return treeRewriter.rewrite(replacement, innerContext);
                }
                return null;
            }
        };

        Row result = ExpressionTreeRewriter.rewriteWith(rewriter, new Row(ImmutableList.of(first, second)), outerContext);
        assertThat(result.items()).containsExactly(replacement, second);
        assertThat(contexts).containsExactly(outerContext, innerContext, outerContext);
    }

    private static List<Expression> references()
    {
        return ImmutableList.of(new Reference(BIGINT, "s0"), new Reference(BIGINT, "s1"), new Reference(BIGINT, "s2"));
    }

    private static ExpressionRewriter<Void> replaceReference(List<Expression> references, int changedIndex, Reference replacement, List<Expression> visited)
    {
        return new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                visited.add(node);
                if (changedIndex >= 0 && node == references.get(changedIndex)) {
                    return replacement;
                }
                return null;
            }
        };
    }

    @Test
    void testEveryExpressionConsidered()
            throws Exception
    {
        assertAllMethodsOverridden(IrVisitor.class, ExpressionTreeRewriter.RewritingVisitor.class, Set.of(
                // process(..) have reasonable defaults
                IrVisitor.class.getMethod("process", Expression.class),
                IrVisitor.class.getMethod("process", Expression.class, Object.class /*context*/)));
    }
}
