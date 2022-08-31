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
import io.trino.sql.planner.rowpattern.ir.IrAnchor.Type;
import io.trino.sql.planner.rowpattern.ir.IrConcatenation;
import io.trino.sql.planner.rowpattern.ir.IrEmpty;
import io.trino.sql.planner.rowpattern.ir.IrExclusion;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrPermutation;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.sql.planner.rowpattern.ir.IrQuantifier;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.AstVisitor;
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
        extends AstVisitor<IrRowPattern, Void>
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

    @Override
    protected IrRowPattern visitPatternAlternation(PatternAlternation node, Void context)
    {
        List<IrRowPattern> patterns = node.getPatterns().stream()
                .map(this::process)
                .collect(toImmutableList());

        return new IrAlternation(patterns);
    }

    @Override
    protected IrRowPattern visitPatternConcatenation(PatternConcatenation node, Void context)
    {
        List<IrRowPattern> patterns = node.getPatterns().stream()
                .map(this::process)
                .collect(toImmutableList());

        return new IrConcatenation(patterns);
    }

    @Override
    protected IrRowPattern visitQuantifiedPattern(QuantifiedPattern node, Void context)
    {
        IrRowPattern pattern = process(node.getPattern());
        IrQuantifier quantifier = rewritePatternQuantifier(node.getPatternQuantifier());

        return new IrQuantified(pattern, quantifier);
    }

    private IrQuantifier rewritePatternQuantifier(PatternQuantifier quantifier)
    {
        if (quantifier instanceof ZeroOrMoreQuantifier) {
            return zeroOrMore(quantifier.isGreedy());
        }

        if (quantifier instanceof OneOrMoreQuantifier) {
            return oneOrMore(quantifier.isGreedy());
        }

        if (quantifier instanceof ZeroOrOneQuantifier) {
            return zeroOrOne(quantifier.isGreedy());
        }

        if (quantifier instanceof RangeQuantifier) {
            Range range = analysis.getRange((RangeQuantifier) quantifier);
            return range(range.getAtLeast(), range.getAtMost(), quantifier.isGreedy());
        }

        throw new IllegalStateException("unsupported pattern quantifier type: " + quantifier.getClass().getSimpleName());
    }

    @Override
    protected IrRowPattern visitAnchorPattern(AnchorPattern node, Void context)
    {
        Type type = switch (node.getType()) {
            case PARTITION_START -> PARTITION_START;
            case PARTITION_END -> PARTITION_END;
        };

        return new IrAnchor(type);
    }

    @Override
    protected IrRowPattern visitEmptyPattern(EmptyPattern node, Void context)
    {
        return new IrEmpty();
    }

    @Override
    protected IrRowPattern visitExcludedPattern(ExcludedPattern node, Void context)
    {
        IrRowPattern pattern = process(node.getPattern());

        return new IrExclusion(pattern);
    }

    @Override
    protected IrRowPattern visitPatternPermutation(PatternPermutation node, Void context)
    {
        List<IrRowPattern> patterns = node.getPatterns().stream()
                .map(this::process)
                .collect(toImmutableList());

        return new IrPermutation(patterns);
    }

    @Override
    protected IrRowPattern visitPatternVariable(PatternVariable node, Void context)
    {
        return new IrLabel(node.getName().getCanonicalValue());
    }
}
