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
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.ExcludedPattern;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternPermutation;
import io.trino.sql.tree.PatternQuantifier;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;

import static java.util.stream.Collectors.joining;

public final class RowPatternFormatter
{
    private RowPatternFormatter() {}

    public static String formatPattern(RowPattern pattern)
    {
        return switch (pattern) {
            case PatternAlternation alternation -> alternation.getPatterns().stream()
                    .map(RowPatternFormatter::formatPattern)
                    .collect(joining(" | ", "(", ")"));
            case PatternConcatenation concatenation -> concatenation.getPatterns().stream()
                    .map(RowPatternFormatter::formatPattern)
                    .collect(joining(" ", "(", ")"));
            case QuantifiedPattern quantified -> "(" + formatPattern(quantified.getPattern()) + formatQuantifier(quantified.getPatternQuantifier()) + ")";
            case PatternVariable variable -> ExpressionFormatter.formatExpression(variable.getName());
            case EmptyPattern _ -> "()";
            case PatternPermutation permutation -> permutation.getPatterns().stream()
                    .map(RowPatternFormatter::formatPattern)
                    .collect(joining(", ", "PERMUTE(", ")"));
            case AnchorPattern anchor -> switch (anchor.getType()) {
                case PARTITION_START -> "^";
                case PARTITION_END -> "$";
            };
            case ExcludedPattern excluded -> "{-" + formatPattern(excluded.getPattern()) + "-}";
        };
    }

    private static String formatQuantifier(PatternQuantifier quantifier)
    {
        String greedy = quantifier.isGreedy() ? "" : "?";
        return switch (quantifier) {
            case ZeroOrMoreQuantifier _ -> "*" + greedy;
            case OneOrMoreQuantifier _ -> "+" + greedy;
            case ZeroOrOneQuantifier _ -> "?" + greedy;
            case RangeQuantifier range -> {
                String atLeast = range.getAtLeast().map(ExpressionFormatter::formatExpression).orElse("");
                String atMost = range.getAtMost().map(ExpressionFormatter::formatExpression).orElse("");
                yield "{" + atLeast + "," + atMost + "}" + greedy;
            }
        };
    }
}
