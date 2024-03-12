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
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Stream;

import static io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static java.util.stream.Collectors.toList;
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
                        new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(new Identifier("x"))), new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("x"))),
                        allocator.getTypes(),
                        allocator))
                .isEqualTo(new BindExpression(
                        ImmutableList.of(new SymbolReference("a")),
                        new LambdaExpression(
                                Stream.of("a_0", "x")
                                        .map(Identifier::new)
                                        .map(LambdaArgumentDeclaration::new)
                                        .collect(toList()),
                                new ArithmeticBinaryExpression(ADD, new SymbolReference("a_0"), new SymbolReference("x")))));
    }
}
