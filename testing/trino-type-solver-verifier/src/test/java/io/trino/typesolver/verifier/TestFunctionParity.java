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
package io.trino.typesolver.verifier;

import io.trino.lib.TrinoPreset;
import io.trino.lib.TypeBridge;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 differential harness, function/operator dimension: resolves the same calls through
 * the solver's Trino preset ({@link TypeLibrary#resolveFunction}) and Trino's own
 * {@link TestingFunctionResolution}, then records where the two disagree on whether the call
 * resolves and on the resolved return type.
 * <p>
 * Like the coercion harness this is a measurement, not a gate: it only asserts that the harness
 * ran. Divergences are written to {@code target/type-solver-parity/function-parity-report.md}.
 */
class TestFunctionParity
{
    private enum Kind
    {
        FUNCTION, OPERATOR
    }

    // Curated call sites across the families the preset claims to model. Operators carry the
    // Trino OperatorType for resolveOperator; functions resolve by name on both sides.
    private static final List<Call> CURATED_CALLS = List.of(
            // --- arithmetic (return type is the interesting part) ---
            op("+", OperatorType.ADD, "integer", "integer"),
            op("+", OperatorType.ADD, "bigint", "integer"),
            op("+", OperatorType.ADD, "double", "integer"),
            op("+", OperatorType.ADD, "real", "bigint"),
            op("+", OperatorType.ADD, "decimal(10,2)", "decimal(10,2)"),
            op("+", OperatorType.ADD, "decimal(10,2)", "decimal(20,4)"),
            op("+", OperatorType.ADD, "decimal(38,0)", "decimal(10,2)"),
            op("+", OperatorType.ADD, "decimal(10,2)", "double"),
            op("-", OperatorType.SUBTRACT, "integer", "bigint"),
            op("-", OperatorType.SUBTRACT, "decimal(10,2)", "decimal(12,4)"),
            op("*", OperatorType.MULTIPLY, "decimal(10,2)", "decimal(10,2)"),
            op("*", OperatorType.MULTIPLY, "integer", "integer"),
            op("/", OperatorType.DIVIDE, "decimal(10,2)", "decimal(10,2)"),
            op("/", OperatorType.DIVIDE, "double", "integer"),
            op("%", OperatorType.MODULO, "integer", "integer"),
            op("%", OperatorType.MODULO, "decimal(10,2)", "decimal(8,4)"),
            // --- comparison / equality (return boolean; tests resolution over coercion) ---
            op("=", OperatorType.EQUAL, "integer", "bigint"),
            op("=", OperatorType.EQUAL, "varchar(5)", "varchar(10)"),
            op("=", OperatorType.EQUAL, "decimal(10,2)", "decimal(12,4)"),
            op("=", OperatorType.EQUAL, "date", "timestamp(3)"),
            op("=", OperatorType.EQUAL, "array(integer)", "array(bigint)"),
            op("<", OperatorType.LESS_THAN, "integer", "double"),
            op("<", OperatorType.LESS_THAN, "varchar(5)", "varchar(5)"),
            op("<=", OperatorType.LESS_THAN_OR_EQUAL, "bigint", "bigint"),
            // --- string functions ---
            fn("concat", "varchar(5)", "varchar(10)"),
            fn("concat", "char(3)", "char(4)"),
            fn("length", "varchar(10)"),
            fn("substr", "varchar(10)", "bigint"),
            fn("substr", "varchar(10)", "bigint", "bigint"),
            fn("substring", "varchar(10)", "bigint"),
            fn("lower", "varchar(10)"),
            fn("upper", "varchar(10)"),
            fn("replace", "varchar(10)", "varchar(2)", "varchar(2)"),
            fn("trim", "varchar(10)"),
            fn("reverse", "varchar(10)"),
            // --- numeric functions ---
            fn("abs", "integer"),
            fn("abs", "decimal(10,2)"),
            fn("abs", "double"),
            fn("ceil", "double"),
            fn("ceil", "decimal(10,2)"),
            fn("floor", "decimal(10,2)"),
            fn("round", "double"),
            fn("round", "decimal(10,2)"),
            fn("round", "double", "integer"),
            fn("power", "double", "double"),
            fn("sqrt", "double"),
            fn("sign", "decimal(10,2)"),
            // --- array / map functions ---
            fn("cardinality", "array(integer)"),
            fn("cardinality", "map(varchar(5), bigint)"),
            fn("element_at", "array(integer)", "bigint"),
            fn("element_at", "map(varchar(5), bigint)", "varchar(5)"),
            fn("array_max", "array(bigint)"),
            fn("array_min", "array(double)"),
            fn("contains", "array(integer)", "integer"),
            fn("array_distinct", "array(integer)"),
            // --- temporal functions ---
            fn("date_add", "varchar(4)", "bigint", "timestamp(3)"),
            fn("date_diff", "varchar(4)", "timestamp(3)", "timestamp(3)"),
            fn("date_trunc", "varchar(4)", "timestamp(3)"),
            fn("year", "timestamp(3)"),
            // --- conditional / misc ---
            fn("coalesce", "integer", "bigint"),
            fn("coalesce", "varchar(5)", "varchar(10)"),
            fn("greatest", "integer", "bigint"),
            fn("greatest", "decimal(10,2)", "decimal(12,4)"),
            fn("least", "double", "integer"),
            fn("least", "varchar(5)", "varchar(10)"),
            fn("nullif", "integer", "bigint"),
            // --- more arithmetic / comparison operators ---
            op("-", OperatorType.SUBTRACT, "date", "date"),
            op("/", OperatorType.DIVIDE, "bigint", "bigint"),
            op("=", OperatorType.EQUAL, "row(integer, varchar(3))", "row(bigint, varchar(5))"),
            op("<=", OperatorType.LESS_THAN_OR_EQUAL, "decimal(10,2)", "decimal(12,4)"),
            // --- more numeric functions ---
            fn("abs", "bigint"),
            fn("ceil", "real"),
            fn("truncate", "double"),
            fn("exp", "double"),
            fn("ln", "double"),
            fn("log10", "double"),
            fn("power", "bigint", "bigint"),
            fn("greatest", "real", "double"),
            // --- more string functions ---
            fn("concat", "varchar(5)", "varchar(10)", "varchar(3)"),
            fn("lpad", "varchar(5)", "bigint", "varchar(2)"),
            fn("rpad", "varchar(5)", "bigint", "varchar(2)"),
            fn("split_part", "varchar(10)", "varchar(2)", "bigint"),
            fn("position", "varchar(5)", "varchar(10)"),
            fn("strpos", "varchar(5)", "varchar(10)"),
            fn("starts_with", "varchar(5)", "varchar(3)"),
            fn("split", "varchar(10)", "varchar(2)"),
            // --- more array / map functions ---
            fn("array_distinct", "array(bigint)"),
            fn("array_sort", "array(integer)"),
            fn("array_position", "array(integer)", "integer"),
            fn("slice", "array(integer)", "bigint", "bigint"),
            fn("array_join", "array(integer)", "varchar(1)"),
            fn("element_at", "array(varchar(5))", "bigint"),
            fn("array_first", "array(bigint)"),
            fn("array_last", "array(integer)"),
            fn("array_remove", "array(integer)", "integer"),
            fn("trim_array", "array(integer)", "bigint"),
            fn("ngrams", "array(varchar(5))", "integer"),
            fn("contains_sequence", "array(bigint)", "array(bigint)"),
            fn("array_histogram", "array(integer)"),
            fn("map_keys", "map(varchar(5), bigint)"),
            fn("map_values", "map(varchar(5), bigint)"),
            // --- more temporal / hash functions ---
            fn("year", "date"),
            fn("date_diff", "varchar(4)", "date", "date"),
            fn("from_unixtime", "double"),
            fn("to_unixtime", "timestamp(3)"),
            fn("md5", "varbinary"),
            fn("sha256", "varbinary"),
            // --- catalog-breadth additions ---
            fn("translate", "varchar(5)", "varchar(2)", "varchar(2)"),
            fn("luhn_check", "varchar(16)"),
            fn("split_to_multimap", "varchar(10)", "varchar(1)", "varchar(1)"),
            fn("word_stem", "varchar(10)"),
            fn("word_stem", "varchar(10)", "varchar(2)"),
            fn("dow", "date"),
            fn("doy", "timestamp(3)"),
            fn("date", "varchar(10)"),
            fn("date", "timestamp(3)"),
            fn("timezone", "timestamp(3) with time zone"),
            fn("to_milliseconds", "interval day to second"),
            fn("from_unixtime_nanos", "bigint"),
            fn("from_iso8601_timestamp_nanos", "varchar(30)"),
            fn("parse_duration", "varchar(10)"),
            fn("from_base", "varchar(5)", "bigint"),
            fn("to_base", "bigint", "bigint"),
            fn("format_number", "double"),
            fn("human_readable_seconds", "double"),
            fn("width_bucket", "double", "double", "double", "bigint"),
            fn("parse_data_size", "varchar(10)"),
            fn("bitwise_right_shift_arithmetic", "integer", "integer"),
            fn("cosine_distance", "array(double)", "array(double)"),
            fn("dot_product", "array(double)", "array(double)"),
            fn("euclidean_distance", "array(double)", "array(double)"),
            fn("normal_cdf", "double", "double", "double"),
            fn("t_cdf", "double", "double"),
            fn("wilson_interval_lower", "bigint", "bigint", "double"),
            fn("variant_is_null", "variant"),
            fn("hmac_sha256", "varbinary", "varbinary"),
            fn("empty_approx_set"),
            fn("hash_counts", "setdigest"),
            fn("intersection_cardinality", "setdigest", "setdigest"),
            fn("jaccard_index", "setdigest", "setdigest"),
            fn("value_at_quantile", "qdigest(bigint)", "double"),
            fn("values_at_quantiles", "qdigest(double)", "array(double)"),
            fn("quantile_at_value", "qdigest(bigint)", "bigint"));

    // A systematic sweep of every arithmetic operator over every ordered pair of numeric types,
    // so the hand-ported decimal precision/scale formulas (add/subtract/multiply/divide/modulo)
    // are checked against Trino exhaustively, not just at curated points.
    private static final List<String> NUMERIC_TYPES = List.of(
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "number",
            "decimal(10,2)",
            "decimal(38,0)",
            "decimal(18,4)",
            "decimal(20,10)",
            "decimal(5,4)",
            "decimal(38,38)");

    private static final List<Call> CALLS = buildCalls();

    private static List<Call> buildCalls()
    {
        List<Call> calls = new ArrayList<>(CURATED_CALLS);
        List<OperatorType> arithmetic = List.of(
                OperatorType.ADD, OperatorType.SUBTRACT, OperatorType.MULTIPLY, OperatorType.DIVIDE, OperatorType.MODULO);
        for (OperatorType operator : arithmetic) {
            String symbol = switch (operator) {
                case ADD -> "+";
                case SUBTRACT -> "-";
                case MULTIPLY -> "*";
                case DIVIDE -> "/";
                case MODULO -> "%";
                default -> throw new IllegalStateException("unexpected operator " + operator);
            };
            for (String left : NUMERIC_TYPES) {
                for (String right : NUMERIC_TYPES) {
                    calls.add(op(symbol, operator, left, right));
                }
            }
        }
        // mod() is the named form of the % operator and must resolve identically across the
        // numeric matrix; sweep it the same way rather than trusting two curated points.
        for (String left : NUMERIC_TYPES) {
            for (String right : NUMERIC_TYPES) {
                calls.add(fn("mod", left, right));
            }
        }
        return List.copyOf(calls);
    }

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final TypeManager typeManager = functionResolution.getPlannerContext().getTypeManager();
    private final TypeLibrary solver = TrinoPreset.library();

    @Test
    void functionParity()
            throws IOException
    {
        List<Divergence> resolutionMismatches = new ArrayList<>();
        List<Divergence> returnTypeMismatches = new ArrayList<>();
        int bothResolve = 0;

        for (Call call : CALLS) {
            List<Type> argumentTypes = call.argumentSignatures().stream()
                    .map(signature -> typeManager.getType(parseTypeDescriptor(signature)))
                    .toList();

            Optional<String> trino = resolveTrino(call, argumentTypes);
            Optional<String> solverReturn = resolveSolver(call, argumentTypes);

            String label = call.label();
            if (trino.isPresent() != solverReturn.isPresent()) {
                resolutionMismatches.add(new Divergence(
                        label,
                        trino.map(value -> "resolved -> " + value).orElse("unresolved"),
                        solverReturn.map(value -> "resolved -> " + value).orElse(solverStatus(call, argumentTypes))));
                continue;
            }
            if (trino.isPresent()) {
                bothResolve++;
                if (!trino.get().equals(solverReturn.orElseThrow())) {
                    returnTypeMismatches.add(new Divergence(label, trino.get(), solverReturn.orElseThrow()));
                }
            }
        }

        Path report = Path.of("target", "type-solver-parity", "function-parity-report.md");
        Files.createDirectories(report.getParent());
        Files.writeString(report, render(CALLS.size(), bothResolve, resolutionMismatches, returnTypeMismatches));

        assertThat(CALLS).isNotEmpty();
        assertThat(report).exists();
    }

    private Optional<String> resolveTrino(Call call, List<Type> argumentTypes)
    {
        try {
            ResolvedFunction resolved = switch (call.kind()) {
                case OPERATOR -> functionResolution.resolveOperator(call.operator(), argumentTypes);
                case FUNCTION -> functionResolution.resolveFunction(call.name(), TypeDescriptorProvider.fromTypes(argumentTypes));
            };
            return Optional.of(TypeBridge.render(TypeBridge.toExpression(resolved.signature().getReturnType())));
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> resolveSolver(Call call, List<Type> argumentTypes)
    {
        return switch (solveOutcome(call, argumentTypes)) {
            case FunctionResolver.Resolved resolved -> Optional.of(TypeBridge.render(resolved.resolution().returnType()));
            case FunctionResolver.ResolutionOutcome _ -> Optional.empty();
        };
    }

    private String solverStatus(Call call, List<Type> argumentTypes)
    {
        return switch (solveOutcome(call, argumentTypes)) {
            case FunctionResolver.NoMatch _ -> "no-match";
            case FunctionResolver.Ambiguous _ -> "ambiguous";
            case FunctionResolver.Incomplete _ -> "incomplete";
            case FunctionResolver.Resolved _ -> "resolved";
        };
    }

    private FunctionResolver.ResolutionOutcome solveOutcome(Call call, List<Type> argumentTypes)
    {
        List<Expression> arguments = argumentTypes.stream().map(TypeBridge::toExpression).toList();
        return solver.resolveFunction(call.name(), arguments);
    }

    private static String render(int total, int bothResolve, List<Divergence> resolutionMismatches, List<Divergence> returnTypeMismatches)
    {
        int returnAgree = bothResolve - returnTypeMismatches.size();
        StringBuilder out = new StringBuilder();
        out.append("# Function/operator parity report (Phase 1)\n\n");
        out.append("Differential resolution of curated calls through the typesolver Trino preset vs Trino's own ");
        out.append("function/operator resolution.\n\n");
        out.append("- Calls: ").append(total).append('\n');
        out.append("- Both engines resolved: ").append(bothResolve).append('\n');
        out.append("- Resolution-status disagreements: ").append(resolutionMismatches.size()).append('\n');
        out.append("- Return-type disagreements (of those both resolved): ").append(returnTypeMismatches.size())
                .append(" (").append(String.format("%.1f%%", bothResolve == 0 ? 100.0 : 100.0 * returnAgree / bothResolve))
                .append(" agree)\n\n");
        appendTable(out, "Resolution-status disagreements", "call", "Trino", "solver", resolutionMismatches);
        appendTable(out, "Return-type disagreements", "call", "Trino return", "solver return", returnTypeMismatches);
        return out.toString();
    }

    private static void appendTable(StringBuilder out, String title, String callHeader, String trinoHeader, String solverHeader, List<Divergence> divergences)
    {
        out.append("## ").append(title).append(" (").append(divergences.size()).append(")\n\n");
        if (divergences.isEmpty()) {
            out.append("_None._\n\n");
            return;
        }
        out.append("| ").append(callHeader).append(" | ").append(trinoHeader).append(" | ").append(solverHeader).append(" |\n");
        out.append("|---|---|---|\n");
        for (Divergence divergence : divergences) {
            out.append("| `").append(divergence.call()).append("` | ").append(divergence.trino()).append(" | ").append(divergence.solver()).append(" |\n");
        }
        out.append('\n');
    }

    private static Call fn(String name, String... argumentSignatures)
    {
        return new Call(Kind.FUNCTION, name, null, List.of(argumentSignatures));
    }

    private static Call op(String name, OperatorType operator, String... argumentSignatures)
    {
        return new Call(Kind.OPERATOR, name, operator, List.of(argumentSignatures));
    }

    private record Call(Kind kind, String name, OperatorType operator, List<String> argumentSignatures)
    {
        String label()
        {
            return name + "(" + String.join(", ", argumentSignatures) + ")";
        }
    }

    private record Divergence(String call, String trino, String solver) {}
}
