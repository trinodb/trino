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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.type.FunctionType;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.and;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEqualityInference
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testDoesNotInferRedundantStraddlingPredicates()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals(add(new Reference(BIGINT, "a1"), new Constant(BIGINT, 1L)), new Constant(BIGINT, 0L)),
                equals(new Reference(BIGINT, "a2"), add(new Reference(BIGINT, "a1"), new Constant(BIGINT, 2L))),
                equals(new Reference(BIGINT, "a1"), add("a3", "b3")),
                equals(new Reference(BIGINT, "b2"), add("a4", "b4")));
        EqualityInference.EqualityPartition partition = inference.generateEqualitiesPartitionedBy(symbols("a1", "a2", "a3", "a4"));
        assertThat(partition.getScopeEqualities()).containsExactly(
                equals(new Constant(BIGINT, 0L), add(new Reference(BIGINT, "a1"), new Constant(BIGINT, 1L))),
                equals(new Reference(BIGINT, "a2"), add(new Reference(BIGINT, "a1"), new Constant(BIGINT, 2L))));
        assertThat(partition.getScopeComplementEqualities()).containsExactly(
                equals(new Constant(BIGINT, 0L), add(new Reference(BIGINT, "b1"), new Constant(BIGINT, 1L))));
        // there shouldn't be equality a2 = b1 + 1 as it can be derived from a2 = a1 + 1, a1 = b1
        assertThat(partition.getScopeStraddlingEqualities()).containsExactly(
                equals("a1", "b1"),
                equals(new Reference(BIGINT, "a1"), add("a3", "b3")),
                equals(new Reference(BIGINT, "b2"), add("a4", "b4")));
    }

    @Test
    public void testTransitivity()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals("d1", "c1"),
                equals("a2", "b2"),
                equals("b2", "a2"),
                equals("b2", "c2"),
                equals("d2", "b2"),
                equals("c2", "d2"));

        assertThat(inference.rewrite(someExpression("a1", "a2"), symbols("d1", "d2"))).isEqualTo(someExpression("d1", "d2"));

        assertThat(inference.rewrite(someExpression("a1", "c1"), symbols("b1"))).isEqualTo(someExpression("b1", "b1"));

        assertThat(inference.rewrite(someExpression("a1", "a2"), symbols("b1", "d2", "c3"))).isEqualTo(someExpression("b1", "d2"));

        // Both starting expressions should canonicalize to the same expression
        assertThat(inference.getScopedCanonical(new Reference(BIGINT, "a2"), matchesSymbols("c2", "d2"))).isEqualTo(inference.getScopedCanonical(new Reference(BIGINT, "b2"), matchesSymbols("c2", "d2")));
        Expression canonical = inference.getScopedCanonical(new Reference(BIGINT, "a2"), matchesSymbols("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        assertThat(inference.rewrite(someExpression("a2", "b2"), symbols("c2", "d2"))).isEqualTo(someExpression(canonical, canonical));
    }

    @Test
    public void testTriviallyRewritable()
    {
        Expression expression = new EqualityInference()
                .rewrite(someExpression("a1", "a2"), symbols("a1", "a2"));

        assertThat(expression).isEqualTo(someExpression("a1", "a2"));
    }

    @Test
    public void testUnrewritable()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals("a2", "b2"));

        assertThat(inference.rewrite(someExpression("a1", "a2"), symbols("b1", "c1"))).isNull();
        assertThat(inference.rewrite(someExpression("c1", "c2"), symbols("a1", "a2"))).isNull();
    }

    @Test
    public void testParseEqualityExpression()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals("a1", "c1"),
                equals("c1", "a1"));

        Expression expression = inference.rewrite(someExpression("a1", "b1"), symbols("c1"));
        assertThat(expression).isEqualTo(someExpression("c1", "c1"));
    }

    @Test
    public void testExtractInferrableEqualities()
    {
        EqualityInference inference = new EqualityInference(
                and(equals("a1", "b1"), equals("b1", "c1"), someExpression("c1", "d1")));

        // Able to rewrite to c1 due to equalities
        assertThat(new Reference(BIGINT, "c1")).isEqualTo(inference.rewrite(new Reference(BIGINT, "a1"), symbols("c1")));

        // But not be able to rewrite to d1 which is not connected via equality
        assertThat(inference.rewrite(new Reference(BIGINT, "a1"), symbols("d1"))).isNull();
    }

    @Test
    public void testEqualityPartitionGeneration()
    {
        EqualityInference inference = new EqualityInference(
                equals(new Reference(BIGINT, "a1"), new Reference(BIGINT, "b1")),
                equals(add("a1", "a1"), multiply(new Reference(BIGINT, "a1"), new Constant(BIGINT, 2L))),
                equals(new Reference(BIGINT, "b1"), new Reference(BIGINT, "c1")),
                equals(add("a1", "a1"), new Reference(BIGINT, "c1")),
                equals(add("a1", "b1"), new Reference(BIGINT, "c1")));

        EqualityInference.EqualityPartition emptyScopePartition = inference.generateEqualitiesPartitionedBy(ImmutableSet.of());
        // Cannot generate any scope equalities with no matching symbols
        assertThat(emptyScopePartition.getScopeEqualities().isEmpty()).isTrue();
        // All equalities should be represented in the inverse scope
        assertThat(emptyScopePartition.getScopeComplementEqualities().isEmpty()).isFalse();
        // There should be no equalities straddling the scope
        assertThat(emptyScopePartition.getScopeStraddlingEqualities().isEmpty()).isTrue();

        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("c1"));

        // There should be equalities in the scope, that only use c1 and are all inferrable equalities
        assertThat(equalityPartition.getScopeEqualities().isEmpty()).isFalse();
        assertThat(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(matchesSymbols("c1")))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // There should be equalities in the inverse scope, that never use c1 and are all inferrable equalities
        assertThat(equalityPartition.getScopeComplementEqualities().isEmpty()).isFalse();
        assertThat(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(matchesSymbols("c1"))))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertThat(equalityPartition.getScopeStraddlingEqualities().isEmpty()).isFalse();
        assertThat(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(matchesSymbols("c1")))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // There should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference(
                ImmutableList.<Expression>builder()
                        .addAll(equalityPartition.getScopeEqualities())
                        .addAll(equalityPartition.getScopeComplementEqualities())
                        .addAll(equalityPartition.getScopeStraddlingEqualities())
                        .build());

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbols("c1"));

        assertThat(setCopy(equalityPartition.getScopeEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeEqualities()));
        assertThat(setCopy(equalityPartition.getScopeComplementEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertThat(setCopy(equalityPartition.getScopeStraddlingEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testMultipleEqualitySetsPredicateGeneration()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals("c1", "d1"),
                equals("a2", "b2"),
                equals("b2", "c2"),
                equals("c2", "d2"));

        // Generating equalities for disjoint groups
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("a1", "a2", "b1", "b2"));

        // There should be equalities in the scope, that only use a* and b* symbols and are all inferrable equalities
        assertThat(equalityPartition.getScopeEqualities().isEmpty()).isFalse();
        assertThat(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(symbolBeginsWith("a", "b")))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // There should be equalities in the inverse scope, that never use a* and b* symbols and are all inferrable equalities
        assertThat(equalityPartition.getScopeComplementEqualities().isEmpty()).isFalse();
        assertThat(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(symbolBeginsWith("a", "b"))))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertThat(equalityPartition.getScopeStraddlingEqualities().isEmpty()).isFalse();
        assertThat(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(symbolBeginsWith("a", "b")))).isTrue();
        assertThat(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference::isInferenceCandidate)).isTrue();

        // Again, there should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference(
                ImmutableList.<Expression>builder()
                        .addAll(equalityPartition.getScopeEqualities())
                        .addAll(equalityPartition.getScopeComplementEqualities())
                        .addAll(equalityPartition.getScopeStraddlingEqualities())
                        .build());

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbols("a1", "a2", "b1", "b2"));

        assertThat(setCopy(equalityPartition.getScopeEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeEqualities()));
        assertThat(setCopy(equalityPartition.getScopeComplementEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertThat(setCopy(equalityPartition.getScopeStraddlingEqualities())).isEqualTo(setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testSubExpressionRewrites()
    {
        EqualityInference inference = new EqualityInference(
                equals(new Reference(BIGINT, "a1"), add("b", "c")), // a1 = b + c
                equals(new Reference(BIGINT, "a2"), multiply(new Reference(BIGINT, "b"), add("b", "c"))), // a2 = b * (b + c)
                equals(new Reference(BIGINT, "a3"), multiply(new Reference(BIGINT, "a1"), add("b", "c")))); // a3 = a1 * (b + c)

        // Expression (b + c) should get entirely rewritten as a1
        assertThat(inference.rewrite(add("b", "c"), symbols("a1", "a2"))).isEqualTo(new Reference(BIGINT, "a1"));

        // Only the sub-expression (b + c) should get rewritten in terms of a*
        assertThat(inference.rewrite(multiply(new Reference(BIGINT, "ax"), add("b", "c")), symbols("ax", "a1", "a2", "a3"))).isEqualTo(multiply(new Reference(BIGINT, "ax"), new Reference(BIGINT, "a1")));

        // To be compliant, could rewrite either the whole expression, or just the sub-expression. Rewriting larger expressions are preferred
        assertThat(inference.rewrite(multiply(new Reference(BIGINT, "a1"), add("b", "c")), symbols("a1", "a2", "a3"))).isEqualTo(new Reference(BIGINT, "a3"));
    }

    @Test
    public void testConstantEqualities()
    {
        EqualityInference inference = new EqualityInference(
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals(new Reference(BIGINT, "c1"), new Constant(BIGINT, 1L)));

        // Should always prefer a constant if available (constant is part of all scopes)
        assertThat(inference.rewrite(new Reference(BIGINT, "a1"), symbols("a1", "b1"))).isEqualTo(new Constant(BIGINT, 1L));

        // All scope equalities should utilize the constant if possible
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("a1", "b1"));
        assertThat(equalitiesAsSets(equalityPartition.getScopeEqualities())).isEqualTo(set(set(new Reference(BIGINT, "a1"), new Constant(BIGINT, 1L)), set(new Reference(BIGINT, "b1"), new Constant(BIGINT, 1L))));
        assertThat(equalitiesAsSets(equalityPartition.getScopeComplementEqualities())).isEqualTo(set(set(new Reference(BIGINT, "c1"), new Constant(BIGINT, 1L))));

        // There should be no scope straddling equalities as the full set of equalities should be already represented by the scope and inverse scope
        assertThat(equalityPartition.getScopeStraddlingEqualities().isEmpty()).isTrue();
    }

    @Test
    public void testEqualityGeneration()
    {
        EqualityInference inference = new EqualityInference(
                equals(new Reference(BIGINT, "a1"), add("b", "c")), // a1 = b + c
                equals(new Reference(BIGINT, "e1"), add("b", "d")), // e1 = b + d
                equals("c", "d"));

        Expression scopedCanonical = inference.getScopedCanonical(new Reference(BIGINT, "e1"), symbolBeginsWith("a"));
        assertThat(scopedCanonical).isEqualTo(new Reference(BIGINT, "a1"));
    }

    @Test
    public void testExpressionsThatMayReturnNullOnNonNullInput()
    {
        List<Expression> candidates = ImmutableList.of(
                new Cast(new Reference(BIGINT, "b"), BIGINT, true), // try_cast
                functionResolution
                        .functionCallBuilder(TryFunction.NAME)
                        .addArgument(new FunctionType(ImmutableList.of(), BIGINT), new Lambda(ImmutableList.of(), new Reference(BIGINT, "b")))
                        .build(),
                new NullIf(new Reference(BIGINT, "b"), number(1)),
                new In(new Reference(BIGINT, "b"), ImmutableList.of(new Constant(BIGINT, null))),
                new Case(ImmutableList.of(new WhenClause(new Not(new IsNull(new Reference(BIGINT, "b"))), new Constant(UnknownType.UNKNOWN, null))), new Constant(UnknownType.UNKNOWN, null)),
                new Switch(new Reference(INTEGER, "b"), ImmutableList.of(new WhenClause(number(1), new Constant(INTEGER, null))), new Constant(INTEGER, null)));

        for (Expression candidate : candidates) {
            EqualityInference inference = new EqualityInference(
                    equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "x")),
                    equals(new Reference(candidate.type(), "a"), candidate));

            List<Expression> equalities = inference.generateEqualitiesPartitionedBy(symbols("b")).getScopeStraddlingEqualities();
            assertThat(equalities.size()).isEqualTo(1);
            assertThat(equalities.get(0).equals(equals(new Reference(BIGINT, "x"), new Reference(BIGINT, "b"))) || equalities.get(0).equals(equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "x")))).isTrue();
        }
    }

    private static Predicate<Expression> matchesSymbolScope(Predicate<Symbol> symbolScope)
    {
        return expression -> Iterables.all(SymbolsExtractor.extractUnique(expression), symbolScope);
    }

    private static Predicate<Expression> matchesStraddlingScope(Predicate<Symbol> symbolScope)
    {
        return expression -> {
            Set<Symbol> symbols = SymbolsExtractor.extractUnique(expression);
            return Iterables.any(symbols, symbolScope) && Iterables.any(symbols, not(symbolScope));
        };
    }

    private static Expression someExpression(String symbol1, String symbol2)
    {
        return someExpression(new Reference(BIGINT, symbol1), new Reference(BIGINT, symbol2));
    }

    private static Expression someExpression(Expression expression1, Expression expression2)
    {
        return new Comparison(GREATER_THAN, expression1, expression2);
    }

    private static Expression add(String symbol1, String symbol2)
    {
        return add(new Reference(BIGINT, symbol1), new Reference(BIGINT, symbol2));
    }

    private static Expression add(Expression expression1, Expression expression2)
    {
        return new Call(ADD_BIGINT, ImmutableList.of(expression1, expression2));
    }

    private static Expression multiply(Expression expression1, Expression expression2)
    {
        return new Call(MULTIPLY_BIGINT, ImmutableList.of(expression1, expression2));
    }

    private static Expression equals(String symbol1, String symbol2)
    {
        return equals(new Reference(BIGINT, symbol1), new Reference(BIGINT, symbol2));
    }

    private static Expression equals(Expression expression1, Expression expression2)
    {
        return new Comparison(EQUAL, expression1, expression2);
    }

    private static Constant number(long number)
    {
        if (number >= Integer.MIN_VALUE && number < Integer.MAX_VALUE) {
            return new Constant(INTEGER, number);
        }

        return new Constant(BIGINT, number);
    }

    private static Set<Symbol> symbols(String... symbols)
    {
        return Arrays.stream(symbols)
                .map(name -> new Symbol(BIGINT, name))
                .collect(toImmutableSet());
    }

    private static Predicate<Symbol> matchesSymbols(String... symbols)
    {
        return matchesSymbols(Arrays.asList(symbols));
    }

    private static Predicate<Symbol> matchesSymbols(Collection<String> symbols)
    {
        Set<Symbol> symbolSet = symbols.stream()
                .map(name -> new Symbol(BIGINT, name))
                .collect(toImmutableSet());

        return Predicates.in(symbolSet);
    }

    private static Predicate<Symbol> symbolBeginsWith(String... prefixes)
    {
        return symbolBeginsWith(Arrays.asList(prefixes));
    }

    private static Predicate<Symbol> symbolBeginsWith(Iterable<String> prefixes)
    {
        return symbol -> {
            for (String prefix : prefixes) {
                if (symbol.name().startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        };
    }

    private static Set<Set<Expression>> equalitiesAsSets(Iterable<Expression> expressions)
    {
        ImmutableSet.Builder<Set<Expression>> builder = ImmutableSet.builder();
        for (Expression expression : expressions) {
            builder.add(equalityAsSet(expression));
        }
        return builder.build();
    }

    private static Set<Expression> equalityAsSet(Expression expression)
    {
        checkArgument(expression instanceof Comparison);
        Comparison comparison = (Comparison) expression;
        checkArgument(comparison.operator() == EQUAL);
        return ImmutableSet.of(comparison.left(), comparison.right());
    }

    @SafeVarargs
    private static <E> Set<E> set(E... elements)
    {
        return ImmutableSet.copyOf(elements);
    }

    private static <E> Set<E> setCopy(Iterable<E> elements)
    {
        return ImmutableSet.copyOf(elements);
    }
}
