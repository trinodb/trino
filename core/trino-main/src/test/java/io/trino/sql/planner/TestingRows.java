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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.Varchars.truncateToLength;
import static io.trino.sql.ir.IrUtils.preOrder;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Fail.fail;

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
    private final ExpressionCompiler compiler;

    public TestingRows(PlannerContext plannerContext, Session session)
    {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(plannerContext, "plannerContext is null");
        this.evaluator = new IrExpressionEvaluator(plannerContext);
        this.compiler = new ExpressionCompiler(
                new PageFunctionCompiler(plannerContext.getFunctionManager(), plannerContext.getMetadata(), plannerContext.getTypeManager(), 0),
                new ColumnarFilterCompiler(plannerContext, 0));
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

    /// Evaluates `expression` for every row, and returns the interpreted value of each, in the order
    /// of `rows`. Along the way the interpreter is checked against the engine that evaluates the
    /// expression in a query, and a disagreement between them fails the test, naming `role` so that
    /// the failure says which expression the engines disagree on.
    ///
    /// [IrExpressionEvaluator] is what a verifier judges a rewrite by, so a defect in it shows up as
    /// a rewrite that looks wrong. Compiled evaluation is an independent second opinion, and it has
    /// to be taken on both sides of a rewrite: the rewritten expression is a shape the original never
    /// had, so it reaches code in either engine that the original does not, and an engine defect
    /// there would otherwise be reported as the rule being wrong.
    ///
    /// Agreement is not correctness, though. Both engines return null for `null IN ()`, where false
    /// is the answer, so no cross-check catches it; see
    /// https://github.com/trinodb/trino/issues/31064.
    public List<Object> evaluateAll(Expression expression, String role, Collection<Symbol> symbols, List<Map<String, Object>> rows)
    {
        List<Object> interpreted = new ArrayList<>();
        for (Map<String, Object> bindings : rows) {
            interpreted.add(evaluate(expression, bindings));
        }

        compiled(expression, symbols, rows).ifPresent(compiled -> {
            for (int row = 0; row < rows.size(); row++) {
                Object interpretedValue = interpreted.get(row);
                Object compiledValue = compiled.get(row);
                if (interpretedValue == EVALUATION_FAILED || compiledValue == EVALUATION_FAILED) {
                    // The engines are free to differ on which work they do, so on what fails: the
                    // compiled `LIKE` skips building its pattern for a null value, where the
                    // interpreter builds it eagerly and reports an invalid escape.
                    continue;
                }
                if (!valuesEqual(expression.type(), interpretedValue, compiledValue)) {
                    fail("the interpreter and the compiled evaluation disagree on %s, so one of the two engines is wrong about that expression on its own; this is an engine defect, not a defect of the rewrite%n  expression:           %s%n  row:                  %s%n  interpreted result:   %s%n  compiled result:      %s",
                            role,
                            expression,
                            formatRow(symbols, rows.get(row)),
                            formatValue(expression.type(), interpretedValue),
                            formatValue(expression.type(), compiledValue));
                }
            }
        });

        return interpreted;
    }

    /// Compares two native values of `type`. Values of a type backed by a block, such as a row or an
    /// array, are not comparable as they are, so they are compared through their object values.
    public static boolean valuesEqual(Type type, Object left, Object right)
    {
        if (Objects.equals(left, right)) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return Objects.equals(objectValue(type, left), objectValue(type, right));
    }

    /// The value of `expression` for every row as the compiled engine evaluates it, or empty when the
    /// compiler does not accept the expression, which leaves nothing to compare against.
    private Optional<List<Object>> compiled(Expression expression, Collection<Symbol> symbols, List<Map<String, Object>> rows)
    {
        List<Symbol> channels = symbols.stream()
                .sorted(comparing(Symbol::name))
                .collect(toImmutableList());
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (int channel = 0; channel < channels.size(); channel++) {
            layout.put(channels.get(channel), channel);
        }

        // Compile the desugared form, which is the shape execution compiles: PlanOptimizers runs
        // DesugarLambdaExpressions first, and the compiler's lambda pre-pass only accepts a lambda
        // whose free variables are its own parameters, so a captured symbol has to reach it as a Bind
        // value. The interpreter is given the literal test IR instead, because that is the shape the
        // rule under test produced; desugaring preserves semantics, so comparing the two is still
        // sound, and the compiled side is if anything the more representative of the two.
        Expression desugared = LambdaCaptureDesugaringRewriter.rewrite(expression, new SymbolAllocator(mentionedSymbols(expression, symbols)));
        checkState(
                symbols.containsAll(SymbolsExtractor.extractUnique(desugared)),
                "desugaring introduced a free symbol outside the layout: %s",
                desugared);

        PageProcessor processor;
        try {
            processor = compiler.compilePageProcessor(session, Optional.empty(), ImmutableList.of(desugared), layout.buildOrThrow()).get();
        }
        catch (RuntimeException _) {
            // the compiler does not accept the expression, so there is no second opinion to compare with
            return Optional.empty();
        }

        try {
            return Optional.of(project(processor, channels, expression.type(), rows));
        }
        catch (RuntimeException _) {
            // a row that fails takes the whole page down with it, so evaluate the rows one by one to
            // tell the row that fails from the rows that would have produced a value
            List<Object> values = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                try {
                    values.add(getOnlyElement(project(processor, channels, expression.type(), ImmutableList.of(row))));
                }
                catch (RuntimeException _) {
                    values.add(EVALUATION_FAILED);
                }
            }
            return Optional.of(values);
        }
    }

    /// Every symbol the expression mentions, the ones bound by a lambda or a `Let` included, together
    /// with `symbols`. A [SymbolAllocator] seeded with these cannot name a capture symbol after a name
    /// already in use, which would quietly change what the expression reads instead of failing.
    private static Set<Symbol> mentionedSymbols(Expression expression, Collection<Symbol> symbols)
    {
        ImmutableSet.Builder<Symbol> mentioned = ImmutableSet.<Symbol>builder().addAll(symbols);
        preOrder(expression).forEach(node -> {
            switch (node) {
                case Reference reference -> mentioned.add(Symbol.from(reference));
                case Lambda lambda -> mentioned.addAll(lambda.arguments());
                case Let let -> mentioned.add(let.name());
                default -> {}
            }
        });
        return mentioned.build();
    }

    /// Runs `rows` through `processor` as a single page, one channel per symbol, and reads the
    /// projected value of every row back.
    private List<Object> project(PageProcessor processor, List<Symbol> channels, Type type, List<Map<String, Object>> rows)
    {
        Block[] blocks = new Block[channels.size()];
        for (int channel = 0; channel < channels.size(); channel++) {
            Symbol symbol = channels.get(channel);
            BlockBuilder builder = symbol.type().createBlockBuilder(null, rows.size());
            for (Map<String, Object> row : rows) {
                writeNativeValue(symbol.type(), builder, row.get(symbol.name()));
            }
            blocks[channel] = builder.build();
        }

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(TestingRows.class.getSimpleName());
        Iterator<Optional<Page>> output = processor.process(session.toConnectorSession(), memoryContext, SourcePage.create(new Page(rows.size(), blocks)));

        List<Object> values = new ArrayList<>();
        while (output.hasNext()) {
            output.next().ifPresent(page -> {
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    values.add(readNativeValue(type, block, position));
                }
            });
        }
        checkState(values.size() == rows.size(), "compiled evaluation produced %s values for %s rows", values.size(), rows.size());
        return values;
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
        if (type instanceof CharType || type instanceof VarcharType) {
            // A string type names no adjacent value, but the values that extend a string by one character
            // are what the two string orderings disagree on: as varchar both 'a\0' and 'a ' follow 'a',
            // while as char 'a\0' precedes 'a', because char comparison pads with spaces, and 'a ' is 'a'.
            Slice slice = (Slice) value;
            for (int codePoint : new int[] {MIN_CODE_POINT, ' '}) {
                Slice extended = Slices.allocate(slice.length() + lengthOfCodePoint(codePoint));
                extended.setBytes(0, slice);
                setCodePointAt(codePoint, extended, slice.length());
                values.add(stringValue(type, extended));
            }
        }
        return values.build();
    }

    /// The representation of a string in `type`: it is cut to the type's length, and a char value carries
    /// no trailing spaces.
    private static Slice stringValue(Type type, Slice slice)
    {
        return switch (type) {
            case CharType charType -> truncateToLengthAndTrimSpaces(slice, charType);
            case VarcharType varcharType -> truncateToLength(slice, varcharType);
            default -> throw new IllegalArgumentException("Not a string type: " + type);
        };
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
        else if (type instanceof CharType || type instanceof VarcharType) {
            STRINGS.stream()
                    .map(Slices::utf8Slice)
                    .map(slice -> stringValue(type, slice))
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
