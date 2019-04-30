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
package io.prestosql.sql.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.type.FunctionType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.EqualityInference.isInferenceCandidate;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEqualityInference
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testTransitivity()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals("d1", "c1"),
                equals("a2", "b2"),
                equals("b2", "a2"),
                equals("b2", "c2"),
                equals("d2", "b2"),
                equals("c2", "d2"));

        assertEquals(
                inference.rewrite(someExpression("a1", "a2"), symbols("d1", "d2")),
                someExpression("d1", "d2"));

        assertEquals(
                inference.rewrite(someExpression("a1", "c1"), symbols("b1")),
                someExpression("b1", "b1"));

        assertEquals(
                inference.rewrite(someExpression("a1", "a2"), symbols("b1", "d2", "c3")),
                someExpression("b1", "d2"));

        // Both starting expressions should canonicalize to the same expression
        assertEquals(
                inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2")),
                inference.getScopedCanonical(nameReference("b2"), matchesSymbols("c2", "d2")));
        Expression canonical = inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        assertEquals(
                inference.rewrite(someExpression("a2", "b2"), symbols("c2", "d2")),
                someExpression(canonical, canonical));
    }

    @Test
    public void testTriviallyRewritable()
    {
        Expression expression = EqualityInference.newInstance(metadata)
                .rewrite(someExpression("a1", "a2"), symbols("a1", "a2"));

        assertEquals(expression, someExpression("a1", "a2"));
    }

    @Test
    public void testUnrewritable()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals("a1", "b1"),
                equals("a2", "b2"));

        assertNull(inference.rewrite(someExpression("a1", "a2"), symbols("b1", "c1")));
        assertNull(inference.rewrite(someExpression("c1", "c2"), symbols("a1", "a2")));
    }

    @Test
    public void testParseEqualityExpression()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals("a1", "b1"),
                equals("a1", "c1"),
                equals("c1", "a1"));

        Expression expression = inference.rewrite(someExpression("a1", "b1"), symbols("c1"));
        assertEquals(expression, someExpression("c1", "c1"));
    }

    @Test
    public void testExtractInferrableEqualities()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                ExpressionUtils.and(equals("a1", "b1"), equals("b1", "c1"), someExpression("c1", "d1")));

        // Able to rewrite to c1 due to equalities
        assertEquals(nameReference("c1"), inference.rewrite(nameReference("a1"), symbols("c1")));

        // But not be able to rewrite to d1 which is not connected via equality
        assertNull(inference.rewrite(nameReference("a1"), symbols("d1")));
    }

    @Test
    public void testEqualityPartitionGeneration()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals(nameReference("a1"), nameReference("b1")),
                equals(add("a1", "a1"), multiply(nameReference("a1"), number(2))),
                equals(nameReference("b1"), nameReference("c1")),
                equals(add("a1", "a1"), nameReference("c1")),
                equals(add("a1", "b1"), nameReference("c1")));

        EqualityInference.EqualityPartition emptyScopePartition = inference.generateEqualitiesPartitionedBy(ImmutableSet.of());
        // Cannot generate any scope equalities with no matching symbols
        assertTrue(emptyScopePartition.getScopeEqualities().isEmpty());
        // All equalities should be represented in the inverse scope
        assertFalse(emptyScopePartition.getScopeComplementEqualities().isEmpty());
        // There should be no equalities straddling the scope
        assertTrue(emptyScopePartition.getScopeStraddlingEqualities().isEmpty());

        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("c1"));

        // There should be equalities in the scope, that only use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(matchesSymbols("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // There should be equalities in the inverse scope, that never use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(matchesSymbols("c1")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(matchesSymbols("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // There should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = EqualityInference.newInstance(
                metadata,
                ImmutableList.<Expression>builder()
                        .addAll(equalityPartition.getScopeEqualities())
                        .addAll(equalityPartition.getScopeComplementEqualities())
                        .addAll(equalityPartition.getScopeStraddlingEqualities())
                        .build());

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbols("c1"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testMultipleEqualitySetsPredicateGeneration()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals("c1", "d1"),
                equals("a2", "b2"),
                equals("b2", "c2"),
                equals("c2", "d2"));

        // Generating equalities for disjoint groups
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("a1", "a2", "b1", "b2"));

        // There should be equalities in the scope, that only use a* and b* symbols and are all inferrable equalities
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(symbolBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // There should be equalities in the inverse scope, that never use a* and b* symbols and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(symbolBeginsWith("a", "b")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(symbolBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), expression -> isInferenceCandidate(metadata, expression)));

        // Again, there should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = EqualityInference.newInstance(
                metadata,
                ImmutableList.<Expression>builder()
                        .addAll(equalityPartition.getScopeEqualities())
                        .addAll(equalityPartition.getScopeComplementEqualities())
                        .addAll(equalityPartition.getScopeStraddlingEqualities())
                        .build());

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbols("a1", "a2", "b1", "b2"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testSubExpressionRewrites()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals(nameReference("a1"), add("b", "c")), // a1 = b + c
                equals(nameReference("a2"), multiply(nameReference("b"), add("b", "c"))), // a2 = b * (b + c)
                equals(nameReference("a3"), multiply(nameReference("a1"), add("b", "c")))); // a3 = a1 * (b + c)

        // Expression (b + c) should get entirely rewritten as a1
        assertEquals(inference.rewrite(add("b", "c"), symbols("a1", "a2")), nameReference("a1"));

        // Only the sub-expression (b + c) should get rewritten in terms of a*
        assertEquals(inference.rewrite(multiply(nameReference("ax"), add("b", "c")), symbols("ax", "a1", "a2", "a3")), multiply(nameReference("ax"), nameReference("a1")));

        // To be compliant, could rewrite either the whole expression, or just the sub-expression. Rewriting larger expressions are preferred
        assertEquals(inference.rewrite(multiply(nameReference("a1"), add("b", "c")), symbols("a1", "a2", "a3")), nameReference("a3"));
    }

    @Test
    public void testConstantEqualities()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals("a1", "b1"),
                equals("b1", "c1"),
                equals(nameReference("c1"), number(1)));

        // Should always prefer a constant if available (constant is part of all scopes)
        assertEquals(inference.rewrite(nameReference("a1"), symbols("a1", "b1")), number(1));

        // All scope equalities should utilize the constant if possible
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbols("a1", "b1"));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeEqualities()),
                set(set(nameReference("a1"), number(1)), set(nameReference("b1"), number(1))));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeComplementEqualities()),
                set(set(nameReference("c1"), number(1))));

        // There should be no scope straddling equalities as the full set of equalities should be already represented by the scope and inverse scope
        assertTrue(equalityPartition.getScopeStraddlingEqualities().isEmpty());
    }

    @Test
    public void testEqualityGeneration()
    {
        EqualityInference inference = EqualityInference.newInstance(
                metadata,
                equals(nameReference("a1"), add("b", "c")), // a1 = b + c
                equals(nameReference("e1"), add("b", "d")), // e1 = b + d
                equals("c", "d"));

        Expression scopedCanonical = inference.getScopedCanonical(nameReference("e1"), symbolBeginsWith("a"));
        assertEquals(scopedCanonical, nameReference("a1"));
    }

    @Test
    public void testExpressionsThatMayReturnNullOnNonNullInput()
    {
        List<Expression> candidates = ImmutableList.of(
                new Cast(nameReference("b"), toSqlType(BIGINT), true), // try_cast
                new FunctionCallBuilder(metadata)
                        .setName(QualifiedName.of(TryFunction.NAME))
                        .addArgument(new FunctionType(ImmutableList.of(), VARCHAR), new LambdaExpression(ImmutableList.of(), nameReference("b")))
                        .build(),
                new NullIfExpression(nameReference("b"), number(1)),
                new IfExpression(nameReference("b"), number(1), new NullLiteral()),
                new DereferenceExpression(nameReference("b"), identifier("x")),
                new InPredicate(nameReference("b"), new InListExpression(ImmutableList.of(new NullLiteral()))),
                new SearchedCaseExpression(ImmutableList.of(new WhenClause(new IsNotNullPredicate(nameReference("b")), new NullLiteral())), Optional.empty()),
                new SimpleCaseExpression(nameReference("b"), ImmutableList.of(new WhenClause(number(1), new NullLiteral())), Optional.empty()),
                new SubscriptExpression(new ArrayConstructor(ImmutableList.of(new NullLiteral())), nameReference("b")));

        for (Expression candidate : candidates) {
            EqualityInference inference = EqualityInference.newInstance(
                    metadata,
                    equals(nameReference("b"), nameReference("x")),
                    equals(nameReference("a"), candidate));

            List<Expression> equalities = inference.generateEqualitiesPartitionedBy(symbols("b")).getScopeStraddlingEqualities();
            assertEquals(equalities.size(), 1);
            assertTrue(equalities.get(0).equals(equals(nameReference("x"), nameReference("b"))) || equalities.get(0).equals(equals(nameReference("b"), nameReference("x"))));
        }
    }

    private static Predicate<Expression> matchesSymbolScope(final Predicate<Symbol> symbolScope)
    {
        return expression -> Iterables.all(SymbolsExtractor.extractUnique(expression), symbolScope);
    }

    private static Predicate<Expression> matchesStraddlingScope(final Predicate<Symbol> symbolScope)
    {
        return expression -> {
            Set<Symbol> symbols = SymbolsExtractor.extractUnique(expression);
            return Iterables.any(symbols, symbolScope) && Iterables.any(symbols, not(symbolScope));
        };
    }

    private static Expression someExpression(String symbol1, String symbol2)
    {
        return someExpression(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression someExpression(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(GREATER_THAN, expression1, expression2);
    }

    private static Expression add(String symbol1, String symbol2)
    {
        return add(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression add(Expression expression1, Expression expression2)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, expression1, expression2);
    }

    private static Expression multiply(String symbol1, String symbol2)
    {
        return multiply(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression multiply(Expression expression1, Expression expression2)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, expression1, expression2);
    }

    private static Expression equals(String symbol1, String symbol2)
    {
        return equals(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(EQUAL, expression1, expression2);
    }

    private static SymbolReference nameReference(String symbol)
    {
        return new SymbolReference(symbol);
    }

    private static LongLiteral number(long number)
    {
        return new LongLiteral(String.valueOf(number));
    }

    private static Set<Symbol> symbols(String... symbols)
    {
        return Arrays.stream(symbols)
                .map(Symbol::new)
                .collect(toImmutableSet());
    }

    private static Predicate<Symbol> matchesSymbols(String... symbols)
    {
        return matchesSymbols(Arrays.asList(symbols));
    }

    private static Predicate<Symbol> matchesSymbols(Collection<String> symbols)
    {
        final Set<Symbol> symbolSet = symbols.stream()
                .map(Symbol::new)
                .collect(toImmutableSet());

        return Predicates.in(symbolSet);
    }

    private static Predicate<Symbol> symbolBeginsWith(String... prefixes)
    {
        return symbolBeginsWith(Arrays.asList(prefixes));
    }

    private static Predicate<Symbol> symbolBeginsWith(final Iterable<String> prefixes)
    {
        return symbol -> {
            for (String prefix : prefixes) {
                if (symbol.getName().startsWith(prefix)) {
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
        Preconditions.checkArgument(expression instanceof ComparisonExpression);
        ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
        Preconditions.checkArgument(comparisonExpression.getOperator() == EQUAL);
        return ImmutableSet.of(comparisonExpression.getLeft(), comparisonExpression.getRight());
    }

    private static <E> Set<E> set(E... elements)
    {
        return setCopy(Arrays.asList(elements));
    }

    private static <E> Set<E> setCopy(Iterable<E> elements)
    {
        return ImmutableSet.copyOf(elements);
    }
}
