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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.assertj.core.api.Assertions.assertThat;

class TestSymbolsExtractor
{
    @Test
    void testLeaves()
    {
        Symbol symbol = new Symbol(BIGINT, "x");
        assertThat(SymbolsExtractor.extractUnique(symbol.toSymbolReference())).containsExactly(symbol);
        assertThat(SymbolsExtractor.extractAll(symbol.toSymbolReference())).containsExactly(symbol);
        assertThat(SymbolsExtractor.extractUnique(new Constant(BIGINT, 1L))).isEmpty();
        assertThat(SymbolsExtractor.extractAll(new Constant(BIGINT, 1L))).isEmpty();
    }

    @Test
    void testDuplicatesAndOrder()
    {
        Symbol first = new Symbol(BIGINT, "b");
        Symbol second = new Symbol(BIGINT, "a");
        Expression expression = new Row(ImmutableList.of(first.toSymbolReference(), second.toSymbolReference(), first.toSymbolReference()));

        assertThat(SymbolsExtractor.extractAll(expression)).containsExactly(first, second, first);
        assertThat(SymbolsExtractor.extractUnique(expression)).containsExactly(first, second);
        assertThat(SymbolsExtractor.extractUnique(ImmutableList.of(expression, second.toSymbolReference())))
                .containsExactly(first, second);
    }

    @Test
    void testNestedLambdas()
    {
        Symbol outerBound = new Symbol(BIGINT, "x");
        Symbol innerBound = new Symbol(BIGINT, "y");
        Symbol free = new Symbol(BIGINT, "z");
        Expression expression = new Lambda(ImmutableList.of(outerBound),
                new Lambda(
                        ImmutableList.of(innerBound),
                        new Row(ImmutableList.of(outerBound.toSymbolReference(), innerBound.toSymbolReference(), free.toSymbolReference(), free.toSymbolReference()))));

        assertThat(SymbolsExtractor.extractAll(expression)).containsExactly(free, free);
        assertThat(SymbolsExtractor.extractUnique(expression)).containsExactly(free);
        assertThat(SymbolsExtractor.extractUnique(ImmutableList.of(expression, outerBound.toSymbolReference(), innerBound.toSymbolReference(), free.toSymbolReference())))
                .containsExactly(free, outerBound, innerBound);
    }

    @Test
    void testBindingsDoNotLeakBetweenExpressions()
    {
        Symbol bound = new Symbol(BIGINT, "x");
        Symbol free = new Symbol(BIGINT, "a");
        Expression body = comparison(EQUAL, bound.toSymbolReference(), free.toSymbolReference());

        assertThat(SymbolsExtractor.extractUnique(ImmutableList.of(
                new Lambda(ImmutableList.of(bound), body),
                bound.toSymbolReference())))
                .containsExactly(free, bound);
        assertThat(SymbolsExtractor.extractUnique(ImmutableList.of(
                new Let(bound, free.toSymbolReference(), body),
                bound.toSymbolReference())))
                .containsExactly(free, bound);
    }

    @Test
    void testLetBoundSymbolIsNotExtracted()
    {
        Symbol bound = new Symbol(BIGINT, "x");
        Symbol outer = new Symbol(BIGINT, "a");
        Let expression = new Let(
                bound,
                new Reference(BIGINT, outer.name()),
                comparison(EQUAL, new Reference(BIGINT, bound.name()), new Reference(BIGINT, outer.name())));

        assertThat(SymbolsExtractor.extractUnique(expression))
                .containsExactly(outer);
        assertThat(SymbolsExtractor.extractAll(expression))
                .containsExactly(outer, outer);
    }

    @Test
    void testLetValueIsEvaluatedInOuterScope()
    {
        Symbol shadowedOuter = new Symbol(BIGINT, "x");
        Let expression = new Let(
                new Symbol(BIGINT, "x"),
                new Reference(BIGINT, shadowedOuter.name()),
                new Reference(BIGINT, "x"));

        assertThat(SymbolsExtractor.extractUnique(expression))
                .containsExactly(shadowedOuter);
        assertThat(SymbolsExtractor.extractAll(expression))
                .containsExactly(shadowedOuter);
    }
}
