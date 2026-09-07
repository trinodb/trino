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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.Varchars.truncateToLength;
import static io.trino.sql.ir.IrUtils.preOrder;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.lang.Boolean.TRUE;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Fail.fail;

/// Verifies the contract of [DomainTranslator] on a generated set of rows: a row passes the original
/// predicate if and only if the extracted [TupleDomain] contains it and the remaining expression is
/// true for it. This holds regardless of how much the translator manages to extract, so it is
/// complementary to asserting the extracted domain, which pins down how much it extracts.
///
/// Rows are built from the literals of the predicate, cast to the type of every symbol, along with
/// their adjacent and the type's edge values. Those are the values a predicate changes its outcome at,
/// which random values would practically never hit.
final class ExtractionResultVerifier
{
    /// Marks an evaluation that failed, e.g. because of an invalid cast. It is not a valid predicate
    /// value, so a row that fails to evaluate is never treated as passing.
    private static final Object FAILURE = new Object();

    /// Cap on the number of rows per verification. Every symbol contributes all of its values, so the
    /// combinations of a predicate over several symbols are sampled down to this many.
    private static final int ROW_BUDGET = 2048;

    /// Step between sampled combinations. It is prime, so it is coprime with the number of combinations
    /// and therefore visits distinct ones, spread over the values of every symbol.
    private static final long SAMPLING_STEP = 2654435761L;

    private static final List<String> STRINGS = ImmutableList.of(
            "",
            " ",
            "  ",
            "0",
            "1",
            "a",
            "A",
            "z",
            "ab",
            "aB",
            " a",
            "a ",
            "a b",
            "ą",
            "🦆",
            "aaaaaaaaaaaaaaaaaaaa");

    private final Session session;
    private final IrExpressionEvaluator evaluator;

    public ExtractionResultVerifier(PlannerContext plannerContext, Session session)
    {
        this.session = requireNonNull(session, "session is null");
        this.evaluator = new IrExpressionEvaluator(requireNonNull(plannerContext, "plannerContext is null"));
    }

    public void verify(Expression predicate, ExtractionResult result)
    {
        // for a non-deterministic predicate, the predicate and the remaining expression are evaluated separately, so their outcomes are unrelated
        checkArgument(isDeterministic(predicate), "predicate is not deterministic: %s", predicate);

        List<Symbol> symbols = SymbolsExtractor.extractUnique(predicate).stream()
                .sorted(comparing(Symbol::name))
                .collect(toImmutableList());
        List<Constant> literals = preOrder(predicate)
                .filter(Constant.class::isInstance)
                .map(Constant.class::cast)
                .collect(toImmutableList());

        List<List<Object>> values = symbols.stream()
                .map(symbol -> values(symbol.type(), literals))
                .collect(toImmutableList());

        long combinations = 1;
        for (List<Object> value : values) {
            combinations = saturatedMultiply(combinations, value.size());
        }
        for (int row = 0; row < min(combinations, ROW_BUDGET); row++) {
            long combination = row * SAMPLING_STEP % combinations;
            Map<String, Object> bindings = new HashMap<>();
            for (int symbol = 0; symbol < symbols.size(); symbol++) {
                List<Object> pool = values.get(symbol);
                bindings.put(symbols.get(symbol).name(), pool.get((int) (combination % pool.size())));
                combination /= pool.size();
            }
            verifyRow(predicate, result, bindings);
        }
    }

    private void verifyRow(Expression predicate, ExtractionResult result, Map<String, Object> bindings)
    {
        Object value = evaluate(predicate, bindings);
        boolean passes = containsRow(result.getTupleDomain(), bindings) && TRUE.equals(evaluate(result.getRemainingExpression(), bindings));

        String problem;
        if (value == FAILURE) {
            // A failure isn't guaranteed, because domain translation may effectively reorder predicate conjuncts.
            // But a failing row cannot become retained one.
            if (!passes) {
                return;
            }
            problem = "the predicate fails for the row, but the translation passes it";
        }
        else if (TRUE.equals(value) == passes) {
            return;
        }
        else {
            problem = TRUE.equals(value) ?
                    "the predicate is true for the row, but the translation does not pass it" :
                    "the predicate is not true for the row, but the translation passes it";
        }

        fail("%s%n  predicate:  %s%n  domain:     %s%n  remaining:  %s%n  row:        %s",
                problem,
                predicate,
                result.getTupleDomain(),
                result.getRemainingExpression(),
                bindings);
    }

    private static boolean containsRow(TupleDomain<Symbol> tupleDomain, Map<String, Object> bindings)
    {
        if (tupleDomain.isNone()) {
            return false;
        }
        for (Map.Entry<Symbol, Domain> entry : tupleDomain.getDomains().orElseThrow().entrySet()) {
            String name = entry.getKey().name();
            checkState(bindings.containsKey(name), "extracted a domain for %s, which the predicate does not reference", name);
            if (!entry.getValue().includesNullableValue(bindings.get(name))) {
                return false;
            }
        }
        return true;
    }

    private List<Object> values(Type type, List<Constant> literals)
    {
        ImmutableSet.Builder<Object> fromLiterals = ImmutableSet.builder();
        for (Constant literal : literals) {
            // a literal is interesting in the type it is written in, so expand it there before casting
            for (Object value : withNeighbors(literal.type(), literal.value())) {
                tryCast(new Constant(literal.type(), value), type).ifPresent(fromLiterals::add);
            }
        }

        ImmutableSet.Builder<Object> values = ImmutableSet.builder();
        Set<Object> gathered = ImmutableSet.builder()
                .addAll(fromLiterals.build())
                .addAll(edgeValues(type))
                .build();
        for (Object value : gathered) {
            values.addAll(withNeighbors(type, value));
        }

        List<Object> result = new ArrayList<>(values.build());
        result.add(null);
        return result;
    }

    /// The value and, when the type is orderable, the values adjacent to it. A predicate over the value
    /// changes its outcome between those, which is what makes them worth generating.
    private static Set<Object> withNeighbors(Type type, Object value)
    {
        if (value == null) {
            return ImmutableSet.of();
        }
        if (!type.isOrderable()) {
            return ImmutableSet.of(value);
        }
        ImmutableSet.Builder<Object> values = ImmutableSet.builder();
        values.add(value);
        type.getPreviousValue(value).ifPresent(values::add);
        type.getNextValue(value).ifPresent(values::add);
        return values.build();
    }

    private static Set<Object> edgeValues(Type type)
    {
        ImmutableSet.Builder<Object> values = ImmutableSet.builder();
        type.getRange().ifPresent(range -> {
            values.add(range.getMin());
            values.add(range.getMax());
        });
        if (type.equals(BOOLEAN)) {
            values.add(true);
            values.add(false);
        }
        else if (type.equals(DOUBLE)) {
            values.add(0.0);
            values.add(-0.0);
            values.add(Double.NaN);
            values.add(Double.POSITIVE_INFINITY);
            values.add(Double.NEGATIVE_INFINITY);
        }
        else if (type.equals(REAL)) {
            values.add((long) floatToRawIntBits(0.0f));
            values.add((long) floatToRawIntBits(-0.0f));
            values.add((long) floatToRawIntBits(Float.NaN));
            values.add((long) floatToRawIntBits(Float.POSITIVE_INFINITY));
            values.add((long) floatToRawIntBits(Float.NEGATIVE_INFINITY));
        }
        else if (type instanceof VarcharType varcharType) {
            STRINGS.stream()
                    .map(Slices::utf8Slice)
                    .map(slice -> truncateToLength(slice, varcharType))
                    .forEach(values::add);
        }
        else if (type instanceof CharType charType) {
            STRINGS.stream()
                    .map(Slices::utf8Slice)
                    // a char value is represented within the type's length and without trailing spaces
                    .map(slice -> truncateToLengthAndTrimSpaces(slice, charType))
                    .forEach(values::add);
        }
        return values.build();
    }

    private static long saturatedMultiply(long left, int right)
    {
        long product = left * right;
        return product / right == left ? product : Long.MAX_VALUE;
    }

    private Optional<Object> tryCast(Constant literal, Type type)
    {
        if (literal.value() == null) {
            return Optional.empty();
        }
        if (literal.type().equals(type)) {
            return Optional.of(literal.value());
        }
        Object value = evaluate(new Cast(literal, type), ImmutableMap.of());
        if (value == FAILURE || value == null) {
            // there is no cast between the types, or the value does not fit the target type
            return Optional.empty();
        }
        return Optional.of(value);
    }

    private Object evaluate(Expression expression, Map<String, Object> bindings)
    {
        try {
            return evaluator.evaluate(expression, session, bindings);
        }
        catch (RuntimeException _) {
            return FAILURE;
        }
    }
}
