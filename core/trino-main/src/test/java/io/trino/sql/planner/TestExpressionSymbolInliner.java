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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static org.assertj.core.api.Assertions.assertThat;

class TestExpressionSymbolInliner
{
    @Test
    void testLetBoundSymbolIsNotInlined()
    {
        Symbol bound = new Symbol(BIGINT, "x");
        Symbol outer = new Symbol(BIGINT, "a");
        // Let(x, a, x = a) with a -> b; the bound x must be left untouched, the outer a inlined.
        Let expression = new Let(
                bound,
                new Reference(BIGINT, outer.name()),
                comparison(EQUAL, new Reference(BIGINT, bound.name()), new Reference(BIGINT, outer.name())));

        assertThat(inlineSymbols(ImmutableMap.of(outer, new Reference(BIGINT, "b")), expression))
                .isEqualTo(new Let(
                        bound,
                        new Reference(BIGINT, "b"),
                        comparison(EQUAL, new Reference(BIGINT, bound.name()), new Reference(BIGINT, "b"))));
    }

    @Test
    void testLetValueIsInlinedInOuterScope()
    {
        // Let(x, x, x) where the value's reference to x is the outer symbol; only it is inlined.
        Symbol shadowedOuter = new Symbol(BIGINT, "x");
        Let expression = new Let(
                new Symbol(BIGINT, "x"),
                new Reference(BIGINT, shadowedOuter.name()),
                new Reference(BIGINT, "x"));

        Expression result = inlineSymbols(ImmutableMap.of(shadowedOuter, new Reference(BIGINT, "y")), expression);
        assertThat(result)
                .isEqualTo(new Let(
                        new Symbol(BIGINT, "x"),
                        new Reference(BIGINT, "y"),
                        new Reference(BIGINT, "x")));
    }
}
