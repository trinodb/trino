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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.Logical.Operator.AND;
import static java.util.Objects.requireNonNull;

/**
 * Decides a filter on one channel from the bounds a page declares for that channel, see
 * {@link SourcePage#getValueBounds(int)}, and runs the filter only for the pages the bounds do not decide.
 * <p>
 * When the bounds lie inside the values the filter accepts, every position passes and the active positions are
 * returned as they came, so the channel is not loaded for the filter and a range of positions stays a range.
 * When the bounds lie outside, no position passes.
 */
public final class PageBoundsFilterEvaluator
        implements FilterEvaluator
{
    enum Decision
    {
        SELECT_ALL,
        SELECT_NONE,
        EVALUATE,
    }

    record ChannelDomain(Symbol symbol, Domain domain) {}

    /**
     * Wraps the evaluator of the expression when the rows it accepts are exactly the rows whose value of one
     * column lies in a domain. Any other evaluator is returned as is.
     */
    static Supplier<FilterEvaluator> boundsAware(Expression expression, Map<Symbol, Integer> layout, Supplier<FilterEvaluator> delegate)
    {
        ChannelDomain channelDomain = extractDomain(expression);
        if (channelDomain == null) {
            return delegate;
        }
        Integer channel = layout.get(channelDomain.symbol());
        if (channel == null) {
            return delegate;
        }
        return () -> new PageBoundsFilterEvaluator(delegate.get(), channel, channelDomain.domain());
    }

    /**
     * The domain of a comparison between a column and a non-null constant, or of a conjunction of such comparisons
     * over one column. The domain never allows null, as a comparison with null does not pass a filter.
     */
    @Nullable
    static ChannelDomain extractDomain(Expression expression)
    {
        if (expression instanceof Logical logical && logical.operator() == AND) {
            ChannelDomain result = null;
            for (Expression term : logical.terms()) {
                ChannelDomain termDomain = extractDomain(term);
                if (termDomain == null) {
                    return null;
                }
                if (result == null) {
                    result = termDomain;
                }
                else if (result.symbol().equals(termDomain.symbol())) {
                    result = new ChannelDomain(result.symbol(), result.domain().intersect(termDomain.domain()));
                }
                else {
                    return null;
                }
            }
            return result;
        }

        Comparison comparison = matchComparison(expression);
        if (comparison == null) {
            return null;
        }
        Reference reference;
        Constant constant;
        boolean referenceOnLeft;
        if (comparison.left() instanceof Reference left && comparison.right() instanceof Constant right) {
            reference = left;
            constant = right;
            referenceOnLeft = true;
        }
        else if (comparison.left() instanceof Constant left && comparison.right() instanceof Reference right) {
            reference = right;
            constant = left;
            referenceOnLeft = false;
        }
        else {
            return null;
        }
        Type type = reference.type();
        Object value = constant.value();
        if (value == null || !type.equals(constant.type()) || !isSupported(type)) {
            return null;
        }
        Range range = switch (comparison) {
            case Comparison.Equal _ -> Range.equal(type, value);
            case Comparison.LessThan _ -> {
                if (referenceOnLeft) {
                    yield Range.lessThan(type, value);
                }
                yield Range.greaterThan(type, value);
            }
            case Comparison.LessThanOrEqual _ -> {
                if (referenceOnLeft) {
                    yield Range.lessThanOrEqual(type, value);
                }
                yield Range.greaterThanOrEqual(type, value);
            }
            case Comparison.Identical _, Comparison.NotEqual _ -> null;
        };
        if (range == null) {
            return null;
        }
        return new ChannelDomain(Symbol.from(reference), Domain.create(ValueSet.ofRanges(range), false));
    }

    // Comparisons of floating point values do not order NaN and the zeros the way a range does
    private static boolean isSupported(Type type)
    {
        return type.isOrderable() && !type.equals(REAL) && !type.equals(DOUBLE);
    }

    private final FilterEvaluator delegate;
    private final int channel;
    private final Domain acceptedValues;

    // Pages of one row group carry the same bounds instance
    @Nullable
    private Domain lastBounds;
    private Decision lastDecision = Decision.EVALUATE;

    PageBoundsFilterEvaluator(FilterEvaluator delegate, int channel, Domain acceptedValues)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.channel = channel;
        this.acceptedValues = requireNonNull(acceptedValues, "acceptedValues is null");
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        long start = System.nanoTime();
        return switch (decide(page)) {
            case SELECT_ALL -> new SelectionResult(activePositions, System.nanoTime() - start);
            case SELECT_NONE -> new SelectionResult(positionsRange(0, 0), System.nanoTime() - start);
            case EVALUATE -> delegate.evaluate(session, activePositions, page);
        };
    }

    Decision decide(SourcePage page)
    {
        Optional<Domain> bounds = page.getValueBounds(channel);
        if (bounds.isEmpty()) {
            return Decision.EVALUATE;
        }
        Domain domain = bounds.get();
        if (domain != lastBounds) {
            lastDecision = decide(domain);
            lastBounds = domain;
        }
        return lastDecision;
    }

    private Decision decide(Domain bounds)
    {
        if (!bounds.getType().equals(acceptedValues.getType())) {
            return Decision.EVALUATE;
        }
        if (acceptedValues.contains(bounds)) {
            return Decision.SELECT_ALL;
        }
        if (!acceptedValues.overlaps(bounds)) {
            return Decision.SELECT_NONE;
        }
        return Decision.EVALUATE;
    }
}
