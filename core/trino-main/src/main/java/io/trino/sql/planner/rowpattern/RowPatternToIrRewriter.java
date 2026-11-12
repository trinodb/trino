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
package io.trino.sql.planner.rowpattern;

import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.Range;
import io.trino.sql.planner.rowpattern.ir.IrAlternation;
import io.trino.sql.planner.rowpattern.ir.IrAnchor;
import io.trino.sql.planner.rowpattern.ir.IrConcatenation;
import io.trino.sql.planner.rowpattern.ir.IrEmpty;
import io.trino.sql.planner.rowpattern.ir.IrExclusion;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrPermutation;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.sql.planner.rowpattern.ir.IrQuantifier;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.rowpattern.ir.IrAnchor.Type.PARTITION_END;
import static io.trino.sql.planner.rowpattern.ir.IrAnchor.Type.PARTITION_START;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.oneOrMore;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.range;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.zeroOrMore;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.zeroOrOne;
import static java.util.Objects.requireNonNull;

public class RowPatternToIrRewriter
{
    private final Analysis analysis;

    public RowPatternToIrRewriter(Analysis analysis)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
    }

    public static IrRowPattern rewrite(RowPattern node, Analysis analysis)
    {
        return new RowPatternToIrRewriter(analysis).process(node);
    }

    private IrRowPattern process(RowPattern node)
    {
        return switch (node) {
            case PatternAlternation alternation -> new IrAlternation(processAll(alternation.getPatterns()));
            case PatternConcatenation concatenation -> new IrConcatenation(processAll(concatenation.getPatterns()));
            case QuantifiedPattern quantified -> new IrQuantified(process(quantified.getPattern()), rewritePatternQuantifier(quantified.getPatternQuantifier()));
            case AnchorPattern anchor -> new IrAnchor(switch (anchor.getType()) {
                case PARTITION_START -> PARTITION_START;
                case PARTITION_END -> PARTITION_END;
            });
            case EmptyPattern _ -> new IrEmpty();
            case ExcludedPattern excluded -> new IrExclusion(process(excluded.getPattern()));
            case PatternPermutation permutation -> new IrPermutation(processAll(permutation.getPatterns()));
            case PatternVariable variable -> new IrLabel(variable.getName().getCanonicalValue());
        };
    }

    private List<IrRowPattern> processAll(List<RowPattern> patterns)
    {
        return patterns.stream()
                .map(this::process)
                .collect(toImmutableList());
    }

    private IrQuantifier rewritePatternQuantifier(PatternQuantifier quantifier)
    {
        return switch (quantifier) {
            case ZeroOrMoreQuantifier _ -> zeroOrMore(quantifier.isGreedy());
            case OneOrMoreQuantifier _ -> oneOrMore(quantifier.isGreedy());
            case ZeroOrOneQuantifier _ -> zeroOrOne(quantifier.isGreedy());
            case RangeQuantifier rangeQuantifier -> {
                Range range = analysis.getRange(rangeQuantifier);
                yield range(range.getAtLeast(), range.getAtMost(), quantifier.isGreedy());
            }
        };
    }
}
