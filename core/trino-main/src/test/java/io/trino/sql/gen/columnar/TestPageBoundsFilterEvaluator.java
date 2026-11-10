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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.PageBoundsFilterEvaluator.ChannelDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageBoundsFilterEvaluator
{
    private static final CharVarcharCoercion CHAR_VARCHAR_COERCION = getCharVarcharCoercion(TEST_SESSION);
    private static final ColumnarFilterCompiler COMPILER = new TestingFunctionResolution().getColumnarFilterCompiler();

    private static final Reference A = new Reference(BIGINT, "a");
    private static final Reference B = new Reference(BIGINT, "b");
    private static final Symbol A_SYMBOL = Symbol.from(A);
    private static final int A_CHANNEL = 0;
    private static final int B_CHANNEL = 1;
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(A_SYMBOL, A_CHANNEL, Symbol.from(B), B_CHANNEL);
    private static final int POSITIONS = 10;

    @Test
    public void testExtractDomain()
    {
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, A, constant(5))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.lessThan(BIGINT, 5L))));
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN_OR_EQUAL, A, constant(5))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.lessThanOrEqual(BIGINT, 5L))));
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(GREATER_THAN, A, constant(5))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.greaterThan(BIGINT, 5L))));
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(GREATER_THAN_OR_EQUAL, A, constant(5))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.greaterThanOrEqual(BIGINT, 5L))));
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(EQUAL, A, constant(5))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.equal(BIGINT, 5L))));
        // the constant may be on either side
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, constant(5), A)))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.greaterThan(BIGINT, 5L))));
        Reference text = new Reference(VARCHAR, "text");
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, text, new Constant(VARCHAR, utf8Slice("m")))))
                .isEqualTo(new ChannelDomain(Symbol.from(text), notNull(Range.lessThan(VARCHAR, utf8Slice("m")))));

        // a conjunction over one column is the intersection of its terms
        assertThat(PageBoundsFilterEvaluator.extractDomain(and(comparison(GREATER_THAN_OR_EQUAL, A, constant(2)), comparison(LESS_THAN, A, constant(5)))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, notNull(Range.range(BIGINT, 2L, true, 5L, false))));
        assertThat(PageBoundsFilterEvaluator.extractDomain(and(comparison(GREATER_THAN, A, constant(5)), comparison(LESS_THAN, A, constant(2)))))
                .isEqualTo(new ChannelDomain(A_SYMBOL, Domain.none(BIGINT)));
    }

    @Test
    public void testNoDomain()
    {
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(NOT_EQUAL, A, constant(5)))).isNull();
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, A, B))).isNull();
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, A, new Constant(BIGINT, null)))).isNull();
        assertThat(PageBoundsFilterEvaluator.extractDomain(and(comparison(LESS_THAN, A, constant(5)), comparison(LESS_THAN, B, constant(5))))).isNull();
        // floating point comparisons do not follow the ordering of a range
        assertThat(PageBoundsFilterEvaluator.extractDomain(comparison(LESS_THAN, new Reference(DOUBLE, "d"), new Constant(DOUBLE, 5.0)))).isNull();
    }

    @Test
    public void testComparison()
    {
        // The page holds 0..9 whatever its bounds claim, so a result that differs from the 5 matching rows shows
        // that the bounds decided the filter and the data was not consulted
        FilterEvaluator evaluator = createEvaluator(comparison(LESS_THAN, A, constant(5)));
        assertThat(evaluator).isInstanceOf(PageBoundsFilterEvaluator.class);

        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of()), 5);
        assertSelectsAll(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 4, false))));
        assertSelectsNone(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 5, 9, false))));

        // bounds that reach across the constant decide nothing
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 9, false))), 5);
        // a null would not pass the comparison
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 4, true))), 5);
        // bounds of another column, and bounds of another type
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of(B_CHANNEL, bounds(BIGINT, 0, 4, false))), 5);
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(INTEGER, 0, 4, false))), 5);
    }

    @Test
    public void testDecisionFollowsBounds()
    {
        FilterEvaluator evaluator = createEvaluator(comparison(LESS_THAN, A, constant(5)));
        Domain inside = bounds(BIGINT, 0, 4, false);
        Domain outside = bounds(BIGINT, 5, 9, false);

        assertSelectsAll(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, inside)));
        assertSelectsAll(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, inside)));
        assertSelectsNone(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, outside)));
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of()), 5);
        assertSelectsAll(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, inside)));
    }

    @Test
    public void testRange()
    {
        FilterEvaluator evaluator = createEvaluator(and(comparison(GREATER_THAN_OR_EQUAL, A, constant(2)), comparison(LESS_THAN, A, constant(5))));
        assertThat(evaluator).isInstanceOf(PageBoundsFilterEvaluator.class);

        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of()), 3);
        assertSelectsAll(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 2, 4, false))));
        assertSelectsNone(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 5, 9, false))));
        assertSelectsNone(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 1, false))));
        assertEvaluated(evaluator, new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 2, 5, false))), 3);
    }

    @Test
    public void testConjunction()
    {
        FilterEvaluator evaluator = createEvaluator(and(comparison(LESS_THAN, A, constant(5)), comparison(GREATER_THAN_OR_EQUAL, B, constant(8))));
        assertThat(evaluator).isInstanceOf(AndFilterEvaluator.class);

        TestingSourcePage page = new TestingSourcePage(ImmutableMap.of());
        assertThat(evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions().size()).isEqualTo(0);
        assertThat(page.loadedChannels()).contains(A_CHANNEL);

        // the term the bounds prove is skipped, and only the other one reads its column
        page = new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 4, false)));
        assertThat(evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions().size()).isEqualTo(2);
        assertThat(page.loadedChannels()).containsExactly(B_CHANNEL);

        // a term the bounds refute decides the conjunction before any column is read, wherever the term stands
        page = new TestingSourcePage(ImmutableMap.of(B_CHANNEL, bounds(BIGINT, 0, 7, false)));
        assertThat(evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions().isEmpty()).isTrue();
        assertThat(page.loadedChannels()).isEmpty();

        // both terms proven
        page = new TestingSourcePage(ImmutableMap.of(A_CHANNEL, bounds(BIGINT, 0, 4, false), B_CHANNEL, bounds(BIGINT, 8, 9, false)));
        SelectedPositions selected = evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions();
        assertThat(selected.isList()).isFalse();
        assertThat(selected.size()).isEqualTo(POSITIONS);
        assertThat(page.loadedChannels()).isEmpty();
    }

    private static void assertSelectsAll(FilterEvaluator evaluator, TestingSourcePage page)
    {
        SelectedPositions selected = evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions();
        assertThat(selected.isList()).isFalse();
        assertThat(selected.getOffset()).isEqualTo(0);
        assertThat(selected.size()).isEqualTo(POSITIONS);
        assertThat(page.loadedChannels()).isEmpty();
    }

    private static void assertSelectsNone(FilterEvaluator evaluator, TestingSourcePage page)
    {
        assertThat(evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions().isEmpty()).isTrue();
        assertThat(page.loadedChannels()).isEmpty();
    }

    private static void assertEvaluated(FilterEvaluator evaluator, TestingSourcePage page, int expectedPositions)
    {
        assertThat(evaluator.evaluate(SESSION, positionsRange(0, POSITIONS), page).selectedPositions().size()).isEqualTo(expectedPositions);
        assertThat(page.loadedChannels()).containsExactly(A_CHANNEL);
    }

    private static FilterEvaluator createEvaluator(Expression filter)
    {
        return createColumnarFilterEvaluator(CHAR_VARCHAR_COERCION, filter, LAYOUT, COMPILER, true, false).orElseThrow().get();
    }

    private static Domain bounds(Type type, long low, long high, boolean nullAllowed)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(type, low, true, high, true)), nullAllowed);
    }

    private static Domain notNull(Range range)
    {
        return Domain.create(ValueSet.ofRanges(range), false);
    }

    private static Expression and(Expression... terms)
    {
        return new Logical(AND, ImmutableList.copyOf(terms));
    }

    private static Constant constant(long value)
    {
        return new Constant(BIGINT, value);
    }

    /**
     * Two bigint channels, each holding 0..9, with declared bounds that need not agree with the data.
     */
    private static final class TestingSourcePage
            implements SourcePage
    {
        private final Map<Integer, Domain> bounds;
        private final Page page = new Page(createLongSequenceBlock(0, POSITIONS), createLongSequenceBlock(0, POSITIONS));
        private final Set<Integer> loadedChannels = new HashSet<>();

        public TestingSourcePage(Map<Integer, Domain> bounds)
        {
            this.bounds = ImmutableMap.copyOf(bounds);
        }

        public Set<Integer> loadedChannels()
        {
            return loadedChannels;
        }

        @Override
        public Optional<Domain> getValueBounds(int channel)
        {
            return Optional.ofNullable(bounds.get(channel));
        }

        @Override
        public int getPositionCount()
        {
            return page.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return page.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return page.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                page.getBlock(channel).retainedBytesForEachPart(consumer);
            }
        }

        @Override
        public int getChannelCount()
        {
            return page.getChannelCount();
        }

        @Override
        public Block getBlock(int channel)
        {
            loadedChannels.add(channel);
            return page.getBlock(channel);
        }

        @Override
        public Page getPage()
        {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                loadedChannels.add(channel);
            }
            return page;
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            throw new UnsupportedOperationException();
        }
    }
}
