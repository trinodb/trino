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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLambdaCaptureDesugaringRewriter
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction("transform", fromTypes(new ArrayType(INTEGER), new FunctionType(ImmutableList.of(INTEGER), INTEGER)));

    @Test
    public void testRewriteBasicLambda()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a")));

        assertThat(
                rewrite(
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "x")),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "x")))),
                        allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(new Reference(INTEGER, "a")),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_0"), new Symbol(INTEGER, "x")),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_0"), new Reference(INTEGER, "x"))))));
    }

    @Test
    public void testRewriteAvoidsLambdaArgumentNameCollision()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a")));

        assertThat(
                rewrite(
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_0")),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "a_0")))),
                        allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(new Reference(INTEGER, "a")),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_1"), new Symbol(INTEGER, "a_0")),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_1"), new Reference(INTEGER, "a_0"))))));
    }

    @Test
    public void testLetBoundSymbolIsNotCaptured()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a")));

        // x -> let b = a + x in b + b
        // `b` is bound within the lambda body, so it is not a free variable and must not be
        // captured. Only `a` is.
        Lambda lambda = new Lambda(
                ImmutableList.of(new Symbol(INTEGER, "x")),
                new Let(new Symbol(INTEGER, "b"),
                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "x"))),
                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "b")))));

        Expression rewritten = rewrite(lambda, allocator);

        assertThat(rewritten).isEqualTo(new Bind(
                ImmutableList.of(new Reference(INTEGER, "a")),
                new Lambda(
                        ImmutableList.of(new Symbol(INTEGER, "a_0"), new Symbol(INTEGER, "x")),
                        new Let(new Symbol(INTEGER, "b"),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_0"), new Reference(INTEGER, "x"))),
                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "b")))))));

        // the rewrite is a fixed point: re-running it must not capture the binding again
        assertThat(rewrite(rewritten, allocator)).isEqualTo(rewritten);
    }

    @Test
    public void testLetBindingDoesNotHideReferenceInValue()
    {
        Symbol symbol = new Symbol(INTEGER, "a");
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(symbol));

        assertThat(rewrite(
                new Lambda(
                        ImmutableList.of(),
                        new Let(symbol, symbol.toSymbolReference(), symbol.toSymbolReference())),
                allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(symbol.toSymbolReference()),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_0")),
                                new Let(symbol, new Reference(INTEGER, "a_0"), symbol.toSymbolReference()))));
    }

    @Test
    public void testLetBoundSymbolFromEnclosingScopeIsCaptured()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a"), new Symbol(INTEGER, "b")));

        // let b = a in (x -> b + x)
        // `b` is bound outside the lambda, so it is a free variable of the lambda and must be
        // captured, but it is no longer free above the Let.
        assertThat(
                rewrite(
                        new Let(new Symbol(INTEGER, "b"),
                                new Reference(INTEGER, "a"),
                                new Lambda(
                                        ImmutableList.of(new Symbol(INTEGER, "x")),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "x"))))),
                        allocator))
                .isEqualTo(new Let(
                        new Symbol(INTEGER, "b"),
                        new Reference(INTEGER, "a"),
                        new Bind(
                                ImmutableList.of(new Reference(INTEGER, "b")),
                                new Lambda(
                                        ImmutableList.of(new Symbol(INTEGER, "b_0"), new Symbol(INTEGER, "x")),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b_0"), new Reference(INTEGER, "x")))))));
    }

    @Test
    public void testLetBoundSymbolCapturedByNestedLambdaIsNotPropagatedPastLet()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a"), new Symbol(INTEGER, "b")));

        // y -> let b = a + y in (x -> b + x)
        // The nested lambda reports `b` as a capture, but `b` is bound by the Let, so it must not be
        // propagated to the enclosing lambda. Only `a` is captured there.
        assertThat(
                rewrite(
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "y")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "y"))),
                                        new Lambda(
                                                ImmutableList.of(new Symbol(INTEGER, "x")),
                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "x")))))),
                        allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(new Reference(INTEGER, "a")),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_1"), new Symbol(INTEGER, "y")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_1"), new Reference(INTEGER, "y"))),
                                        new Bind(
                                                ImmutableList.of(new Reference(INTEGER, "b")),
                                                new Lambda(
                                                        ImmutableList.of(new Symbol(INTEGER, "b_0"), new Symbol(INTEGER, "x")),
                                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b_0"), new Reference(INTEGER, "x")))))))));
    }

    @Test
    public void testNestedLets()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a")));

        // x -> let b = a + x in let c = b + x in c + c
        // Neither binding escapes: only `a` is captured.
        assertThat(
                rewrite(
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "x")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "x"))),
                                        new Let(new Symbol(INTEGER, "c"),
                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "x"))),
                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "c"), new Reference(INTEGER, "c")))))),
                        allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(new Reference(INTEGER, "a")),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_0"), new Symbol(INTEGER, "x")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_0"), new Reference(INTEGER, "x"))),
                                        new Let(new Symbol(INTEGER, "c"),
                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "x"))),
                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "c"), new Reference(INTEGER, "c"))))))));
    }

    @Test
    public void testLetBoundSymbolReferencedDirectlyAndByNestedLambda()
    {
        SymbolAllocator allocator = new SymbolAllocator(ImmutableList.of(new Symbol(INTEGER, "a"), new Symbol(INTEGER, "b")));

        // y -> let b = a + y in transform(ARRAY[b], x -> b + x)
        // The direct reference is suppressed while the binding is in scope, and the one the nested
        // lambda captures is removed at the Let. Neither reaches the enclosing lambda.
        assertThat(
                rewrite(
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "y")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "y"))),
                                        new Call(TRANSFORM, ImmutableList.of(
                                                new Array(INTEGER, ImmutableList.of(new Reference(INTEGER, "b"))),
                                                new Lambda(
                                                        ImmutableList.of(new Symbol(INTEGER, "x")),
                                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Reference(INTEGER, "x")))))))),
                        allocator))
                .isEqualTo(new Bind(
                        ImmutableList.of(new Reference(INTEGER, "a")),
                        new Lambda(
                                ImmutableList.of(new Symbol(INTEGER, "a_1"), new Symbol(INTEGER, "y")),
                                new Let(new Symbol(INTEGER, "b"),
                                        new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a_1"), new Reference(INTEGER, "y"))),
                                        new Call(TRANSFORM, ImmutableList.of(
                                                new Array(INTEGER, ImmutableList.of(new Reference(INTEGER, "b"))),
                                                new Bind(
                                                        ImmutableList.of(new Reference(INTEGER, "b")),
                                                        new Lambda(
                                                                ImmutableList.of(new Symbol(INTEGER, "b_0"), new Symbol(INTEGER, "x")),
                                                                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b_0"), new Reference(INTEGER, "x")))))))))));
    }
}
