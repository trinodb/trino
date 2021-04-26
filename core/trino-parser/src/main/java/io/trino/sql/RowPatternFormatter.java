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

import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.ExcludedPattern;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternPermutation;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class RowPatternFormatter
{
    private RowPatternFormatter() {}

    public static String formatPattern(RowPattern pattern)
    {
        return new Formatter().process(pattern, null);
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitRowPattern(RowPattern node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitPatternAlternation(PatternAlternation node, Void context)
        {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(" | ", "(", ")"));
        }

        @Override
        protected String visitPatternConcatenation(PatternConcatenation node, Void context)
        {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(" ", "(", ")"));
        }

        @Override
        protected String visitQuantifiedPattern(QuantifiedPattern node, Void context)
        {
            return "(" + process(node.getPattern(), context) + process(node.getPatternQuantifier(), context) + ")";
        }

        @Override
        protected String visitPatternVariable(PatternVariable node, Void context)
        {
            return ExpressionFormatter.formatExpression(node.getName());
        }

        @Override
        protected String visitEmptyPattern(EmptyPattern node, Void context)
        {
            return "()";
        }

        @Override
        protected String visitPatternPermutation(PatternPermutation node, Void context)
        {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(", ", "PERMUTE(", ")"));
        }

        @Override
        protected String visitAnchorPattern(AnchorPattern node, Void context)
        {
            switch (node.getType()) {
                case PARTITION_START:
                    return "^";
                case PARTITION_END:
                    return "$";
                default:
                    throw new IllegalStateException("unexpected anchor pattern type: " + node.getType());
            }
        }

        @Override
        protected String visitExcludedPattern(ExcludedPattern node, Void context)
        {
            return "{-" + process(node.getPattern(), context) + "-}";
        }

        @Override
        protected String visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, Void context)
        {
            String greedy = node.isGreedy() ? "" : "?";
            return "*" + greedy;
        }

        @Override
        protected String visitOneOrMoreQuantifier(OneOrMoreQuantifier node, Void context)
        {
            String greedy = node.isGreedy() ? "" : "?";
            return "+" + greedy;
        }

        @Override
        protected String visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, Void context)
        {
            String greedy = node.isGreedy() ? "" : "?";
            return "?" + greedy;
        }

        @Override
        protected String visitRangeQuantifier(RangeQuantifier node, Void context)
        {
            String greedy = node.isGreedy() ? "" : "?";
            String atLeast = node.getAtLeast().map(ExpressionFormatter::formatExpression).orElse("");
            String atMost = node.getAtMost().map(ExpressionFormatter::formatExpression).orElse("");
            return "{" + atLeast + "," + atMost + "}" + greedy;
        }
    }
}
