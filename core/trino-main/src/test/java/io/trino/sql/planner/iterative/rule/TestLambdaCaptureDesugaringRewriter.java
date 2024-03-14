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
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.BindExpression;
import io.trino.sql.ir.LambdaExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLambdaCaptureDesugaringRewriter
{
    @Test
    public void testRewriteBasicLambda()
    {
        Map<Symbol, Type> symbols = ImmutableMap.of(new Symbol("a"), BigintType.BIGINT);
        SymbolAllocator allocator = new SymbolAllocator(symbols);

        assertThat(
                rewrite(
                        new LambdaExpression(ImmutableList.of("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("x"))),
                        allocator.getTypes(),
                        allocator))
                .isEqualTo(new BindExpression(
                        ImmutableList.of(new SymbolReference("a")),
                        new LambdaExpression(
                                ImmutableList.of("a_0", "x"),
                                new ArithmeticBinaryExpression(ADD, new SymbolReference("a_0"), new SymbolReference("x")))));
    }
}
