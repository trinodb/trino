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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Stream;

import static io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestLambdaCaptureDesugaringRewriter
{
    @Test
    public void testRewriteBasicLambda()
    {
        Map<Symbol, Type> symbols = ImmutableMap.of(new Symbol("a"), BigintType.BIGINT);
        SymbolAllocator allocator = new SymbolAllocator(symbols);

        assertEquals(rewrite(expression("x -> a + x"), allocator.getTypes(), allocator),
                new BindExpression(
                        ImmutableList.of(expression("a")),
                        new LambdaExpression(
                                Stream.of("a_0", "x")
                                        .map(Identifier::new)
                                        .map(LambdaArgumentDeclaration::new)
                                        .collect(toList()),
                                expression("a_0 + x"))));
    }
}
