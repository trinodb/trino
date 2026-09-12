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
import java.util.Set;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential parity for Trino's <em>calculated</em> scalar signatures — the ones whose result
 * type is computed from the argument literals via {@code longVariableConstraints} and
 * {@code TypeCalculation} inside {@link io.trino.metadata.SignatureBinder}. These (decimal
 * precision/scale; varchar length) are the only resolution machinery the typesolver preset
 * re-implements by hand rather than inheriting, so they are the part most prone to silent formula
 * drift (a decimal-multiply off-by-one lived here once).
 * <p>
 * For every calculated builtin the preset models, this sweeps a grid of argument types through
 * both the solver and Trino's full resolver (which runs SignatureBinder) and compares the bound
 * return type. The named builtins carrying calculated signatures are {@code ceiling}, {@code floor},
 * {@code round}, {@code truncate}, {@code mod} (decimal) and {@code concat}, {@code replace},
 * {@code regexp_replace}, {@code url_encode}, {@code to_iso8601}, {@code render} (varchar); the
 * arithmetic operators are calculated too and are swept by {@link TestFunctionParity}.
 */
class TestCalculatedSignatureParity
{
    private static final List<String> DECIMALS = List.of(
            "decimal(10,2)", "decimal(38,0)", "decimal(18,4)", "decimal(5,4)", "decimal(38,38)");
    private static final List<String> VARCHARS = List.of("varchar(5)", "varchar(50)", "varchar(2000)");
    private static final List<String> TIMESTAMPS = List.of("timestamp(0)", "timestamp(3)", "timestamp(6)", "timestamp(9)");

    /**
     * Calculated builtins whose result the solver deliberately does not reproduce:
     * <ul>
     *   <li>{@code concat(char, char)} — blocked on the char/varchar coercion-direction plan;
     *       the solver leaves it incomplete rather than adopt the current (disputed) direction.</li>
     *   <li>{@code render} — over the cast-isolated {@code color} type, intentionally unmodeled.</li>
     * </ul>
     */
    private static final Set<String> DELIBERATELY_DIVERGENT = Set.of("concat", "render");

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final TypeManager typeManager = functionResolution.getPlannerContext().getTypeManager();
    private final TypeLibrary solver = TrinoPreset.library();

    @Test
    void calculatedSignatureParity()
            throws IOException
    {
        List<Call> calls = new ArrayList<>();
        for (String d : DECIMALS) {
            calls.add(new Call("ceiling", List.of(d)));
            calls.add(new Call("floor", List.of(d)));
            calls.add(new Call("round", List.of(d)));
            calls.add(new Call("round", List.of(d, "integer")));
            calls.add(new Call("truncate", List.of(d)));
            for (String e : DECIMALS) {
                calls.add(new Call("mod", List.of(d, e)));
            }
        }
        for (String v : VARCHARS) {
            calls.add(new Call("url_encode", List.of(v)));
            calls.add(new Call("regexp_replace", List.of(v, "varchar(2)")));
            for (String w : VARCHARS) {
                calls.add(new Call("replace", List.of(v, "varchar(2)", w)));
                calls.add(new Call("regexp_replace", List.of(v, "varchar(2)", w)));
                calls.add(new Call("concat", List.of(v, w)));
            }
        }
        calls.add(new Call("concat", List.of("char(3)", "char(4)")));
        for (String ts : TIMESTAMPS) {
            calls.add(new Call("to_iso8601", List.of(ts)));
            calls.add(new Call("to_iso8601", List.of(ts + " with time zone")));
        }
        calls.add(new Call("to_iso8601", List.of("date")));

        List<String> divergences = new ArrayList<>();
        int agree = 0;
        for (Call call : calls) {
            List<Type> argumentTypes = call.arguments().stream()
                    .map(signature -> typeManager.getType(parseTypeDescriptor(signature)))
                    .toList();
            Optional<String> trino = resolveTrino(call, argumentTypes);
            Optional<String> solved = resolveSolver(call, argumentTypes);
            if (trino.equals(solved)) {
                agree++;
            }
            else {
                divergences.add("%s :: trino=%s solver=%s".formatted(
                        call.label(), trino.orElse("unresolved"), solved.orElse("unresolved")));
            }
        }

        StringBuilder report = new StringBuilder();
        report.append("# Calculated-signature parity report (Phase 1)\n\n");
        report.append("Solver vs Trino (SignatureBinder) bound return type for calculated scalar signatures.\n\n");
        report.append("- calls: ").append(calls.size()).append('\n');
        report.append("- agree: ").append(agree).append('\n');
        report.append("- divergences: ").append(divergences.size()).append("\n\n");
        divergences.forEach(d -> report.append("- ").append(d).append('\n'));
        Path path = Path.of("target", "type-solver-parity", "calculated-signature-report.md");
        Files.createDirectories(path.getParent());
        Files.writeString(path, report.toString());

        // Every remaining divergence must be a documented, deliberate exclusion.
        List<String> unexpected = divergences.stream()
                .filter(d -> DELIBERATELY_DIVERGENT.stream().noneMatch(name -> d.startsWith(name + "(")))
                .toList();
        assertThat(unexpected)
                .as("solver must reproduce SignatureBinder's calculated return type for these builtins")
                .isEmpty();
    }

    private Optional<String> resolveTrino(Call call, List<Type> argumentTypes)
    {
        try {
            ResolvedFunction resolved = functionResolution.resolveFunction(call.name(), TypeDescriptorProvider.fromTypes(argumentTypes));
            return Optional.of(TypeBridge.render(TypeBridge.toExpression(resolved.signature().getReturnType())));
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> resolveSolver(Call call, List<Type> argumentTypes)
    {
        List<Expression> arguments = argumentTypes.stream().map(TypeBridge::toExpression).toList();
        return switch (solver.resolveFunction(call.name(), arguments)) {
            case FunctionResolver.Resolved resolved -> Optional.of(TypeBridge.render(resolved.resolution().returnType()));
            case FunctionResolver.ResolutionOutcome _ -> Optional.empty();
        };
    }

    private record Call(String name, List<String> arguments)
    {
        String label()
        {
            return name + "(" + String.join(", ", arguments) + ")";
        }
    }
}
