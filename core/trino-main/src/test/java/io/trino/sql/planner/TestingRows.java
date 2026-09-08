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
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.Varchars.truncateToLength;
import static io.trino.sql.ir.IrUtils.preOrder;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/// Generates rows to evaluate expressions on, for tests that verify a rewrite against the original
/// expression rather than against a frozen expected output.
///
/// Values are built from the literals of a source expression, cast to the type of every symbol, along
/// with their adjacent and the type's edge values. Those are the values an expression changes its
/// outcome at, which random values would practically never hit. The rows are the combinations of
/// those values, sampled down to a fixed budget, and depend only on the input, so a failure reproduces.
public final class TestingRows
{
    /// Marks an evaluation that failed, e.g. because of an invalid cast. It is not a value any
    /// expression can produce, so it never compares equal to a successful evaluation.
    public static final Object EVALUATION_FAILED = new Object();

    /// Cap on the number of generated rows. Every symbol contributes all of its values, so the
    /// combinations over several symbols are sampled down to this many.
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

    public TestingRows(PlannerContext plannerContext, Session session)
    {
        this.session = requireNonNull(session, "session is null");
        this.evaluator = new IrExpressionEvaluator(requireNonNull(plannerContext, "plannerContext is null"));
    }

    /// True when `symbols` uses one name for more than one type. The expression they come from is then
    /// ill-typed, so it cannot occur in a plan and there is no single type to generate values of. A
    /// verifier skips such an expression rather than failing on it.
    public static boolean hasConflictingTypes(Collection<Symbol> symbols)
    {
        return symbols.stream().map(Symbol::name).distinct().count() != symbols.stream().distinct().count();
    }

    /// Rows over the free symbols of `expression`, with the values drawn from its own literals.
    public List<Map<String, Object>> rows(Expression expression)
    {
        return rows(SymbolsExtractor.extractUnique(expression), ImmutableList.of(expression));
    }

    /// Rows over `symbols`, with the values drawn from the literals of `literalSources`. The symbols
    /// need not occur in the sources: a rewrite may drop a symbol, and the row still has to bind it.
    public List<Map<String, Object>> rows(Collection<Symbol> symbols, Collection<Expression> literalSources)
    {
        List<Symbol> ordered = symbols.stream()
                .sorted(comparing(Symbol::name))
                .collect(toImmutableList());
        checkArgument(!hasConflictingTypes(ordered), "symbols contain the same name with different types: %s", symbols);

        List<Constant> literals = literalSources.stream()
                .flatMap(source -> preOrder(source)
                        .filter(Constant.class::isInstance)
                        .map(Constant.class::cast))
                .collect(toImmutableList());

        List<List<Object>> values = ordered.stream()
                .map(symbol -> values(symbol.type(), literals))
                .collect(toImmutableList());

        long combinations = 1;
        for (List<Object> value : values) {
            combinations = saturatedMultiply(combinations, value.size());
        }

        ImmutableList.Builder<Map<String, Object>> rows = ImmutableList.builder();
        for (int row = 0; row < min(combinations, ROW_BUDGET); row++) {
            long combination = row * SAMPLING_STEP % combinations;
            Map<String, Object> bindings = new HashMap<>();
            for (int symbol = 0; symbol < ordered.size(); symbol++) {
                List<Object> pool = values.get(symbol);
                bindings.put(ordered.get(symbol).name(), pool.get((int) (combination % pool.size())));
                combination /= pool.size();
            }
            rows.add(bindings);
        }
        return rows.build();
    }

    /// Renders a value the way a query result shows it. The native representation of many types is an
    /// opaque `Slice` or `Int128`, which says nothing in a failure message.
    public static String formatValue(Type type, Object value)
    {
        if (value == EVALUATION_FAILED) {
            return "<evaluation failed>";
        }
        if (value == null) {
            return "null";
        }
        Object objectValue = objectValue(type, value);
        // quoted, so that a value that is blank or has significant whitespace is still readable
        return objectValue instanceof CharSequence ? "'" + objectValue + "'" : String.valueOf(objectValue);
    }

    /// Renders a row as the values its symbols are bound to, ordered by symbol name.
    public static String formatRow(Collection<Symbol> symbols, Map<String, Object> bindings)
    {
        return symbols.stream()
                .sorted(comparing(Symbol::name))
                .map(symbol -> symbol.name() + "=" + formatValue(symbol.type(), bindings.get(symbol.name())))
                .collect(joining(", ", "{", "}"));
    }

    /// The value of a native value as [Type#getObjectValue] returns it.
    public static Object objectValue(Type type, Object value)
    {
        return type.getObjectValue(writeNativeValue(type, value), 0);
    }

    /// Evaluates `expression` for a row, reporting a failure as [#EVALUATION_FAILED] rather than
    /// throwing. A `null` result is a value like any other, so it is returned as such.
    public Object evaluate(Expression expression, Map<String, Object> bindings)
    {
        try {
            return evaluator.evaluate(expression, session, bindings);
        }
        catch (RuntimeException _) {
            return EVALUATION_FAILED;
        }
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
        if (value == EVALUATION_FAILED || value == null) {
            // there is no cast between the types, or the value does not fit the target type
            return Optional.empty();
        }
        return Optional.of(value);
    }
}
