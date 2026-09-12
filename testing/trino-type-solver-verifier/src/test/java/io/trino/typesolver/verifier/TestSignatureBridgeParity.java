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

import io.trino.lib.SignatureBridge;
import io.trino.lib.TrinoPreset;
import io.trino.lib.TrinoSpecificity;
import io.trino.lib.TypeBridge;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.Specificity;
import org.weakref.solver.TypeLibrary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2a validation: feed the solver from Trino's live function catalog (via {@link SignatureBridge})
 * instead of the hand-written preset, and check that calls resolve the same way Trino resolves them.
 * Reuses the preset's type catalog and coercion/cast rules — only the function definitions come from
 * {@code GlobalFunctionCatalog}.
 */
class TestSignatureBridgeParity
{
    private final TestingFunctionResolution functions = new TestingFunctionResolution();
    private final TypeManager typeManager = functions.getPlannerContext().getTypeManager();

    private static final List<Call> CALLS = List.of(
            new Call("abs", List.of("bigint")),
            new Call("abs", List.of("decimal(10,2)")),
            new Call("length", List.of("varchar(5)")),
            new Call("lower", List.of("varchar(10)")),
            new Call("sqrt", List.of("double")),
            new Call("array_distinct", List.of("array(bigint)")),
            new Call("element_at", List.of("array(varchar(5))", "bigint")),
            new Call("map_keys", List.of("map(varchar(5), bigint)")),
            new Call("from_unixtime", List.of("double")),
            new Call("greatest", List.of("integer", "bigint")),
            new Call("concat", List.of("varchar(5)", "varchar(10)")),
            new Call("round", List.of("decimal(18,4)")),
            new Call("mod", List.of("decimal(10,2)", "tinyint")));

    @Test
    void catalogFedResolutionMatchesTrino()
            throws IOException
    {
        // Build a library from the live catalog: preset types/coercions/casts, bridged functions.
        TypeLibrary.Builder builder = TypeLibrary.builder();
        TrinoPreset.typeConstructors().forEach(builder::registerType);
        TrinoPreset.coercionRules().forEach(builder::registerCoercion);
        TrinoPreset.castRules().forEach(builder::registerCast);
        builder.withSpecificity(Specificity.BY_COERCION_COUNT.then(new TrinoSpecificity(TrinoPreset.typeSystem())));

        TreeSet<String> bridgedNames = new TreeSet<>();
        int signatures = 0;
        int bridgeFailures = 0;
        int calculated = 0;
        for (FunctionMetadata metadata : functions.listGlobalFunctions()) {
            if (metadata.getKind() != FunctionKind.SCALAR || metadata.isHidden() || metadata.getCanonicalName().startsWith("$")) {
                continue;
            }
            signatures++;
            if (SignatureBridge.isCalculated(metadata.getSignature())) {
                calculated++;
            }
            try {
                for (String name : metadata.getNames()) {
                    if (name.startsWith("$")) {
                        continue;
                    }
                    builder.registerFunction(name, SignatureBridge.toTypeScheme(metadata.getSignature()));
                    bridgedNames.add(name);
                }
            }
            catch (RuntimeException e) {
                bridgeFailures++;
            }
        }
        TypeLibrary catalogFed = builder.build();

        StringBuilder report = new StringBuilder("# Signature-bridge parity (Phase 2a)\n\n");
        report.append("- scalar signatures: ").append(signatures).append('\n');
        report.append("- bridged distinct names: ").append(bridgedNames.size()).append('\n');
        report.append("- bridge failures (threw): ").append(bridgeFailures).append('\n');
        report.append("- calculated signatures (longVariableConstraints, modelled via the type-calculation parser): ").append(calculated).append("\n\n");
        report.append("## Sample call resolution (catalog-fed solver vs Trino)\n\n");
        report.append("| call | trino | catalog-fed solver |\n|---|---|---|\n");

        int agree = 0;
        for (Call call : CALLS) {
            List<Type> argumentTypes = call.arguments().stream()
                    .map(signature -> typeManager.getType(parseTypeDescriptor(signature)))
                    .toList();
            Optional<String> trino = resolveTrino(call.name(), argumentTypes);
            Optional<String> solver = resolveSolver(catalogFed, call.name(), argumentTypes);
            if (trino.equals(solver)) {
                agree++;
            }
            report.append("| `").append(call.label()).append("` | ")
                    .append(trino.orElse("unresolved")).append(" | ")
                    .append(solver.orElse("unresolved")).append(" |\n");
        }
        report.insert(report.indexOf("## Sample"), "- sample calls agreeing with Trino: " + agree + "/" + CALLS.size() + "\n\n");

        Path path = Path.of("target", "type-solver-parity", "signature-bridge-report.md");
        Files.createDirectories(path.getParent());
        Files.writeString(path, report.toString());

        // Every signature translates structurally, the bridge spans the whole catalog, and a
        // catalog-fed solver matches Trino on every sample call — including the calculated ones
        // (round/mod via the type-calculation parser) and concat, which resolves to the unbounded
        // varchar overload once unbounded varchar is bridged as a distinct type.
        assertThat(bridgeFailures).isZero();
        assertThat(bridgedNames.size()).isGreaterThan(250);
        assertThat(agree).isEqualTo(CALLS.size());
    }

    private Optional<String> resolveTrino(String name, List<Type> argumentTypes)
    {
        try {
            ResolvedFunction resolved = functions.resolveFunction(name, TypeDescriptorProvider.fromTypes(argumentTypes));
            return Optional.of(TypeBridge.render(TypeBridge.toExpression(resolved.signature().getReturnType())));
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> resolveSolver(TypeLibrary library, String name, List<Type> argumentTypes)
    {
        List<Expression> arguments = argumentTypes.stream().map(TypeBridge::toExpression).toList();
        return switch (library.resolveFunction(name, arguments)) {
            case FunctionResolver.Resolved resolved -> Optional.of(TypeBridge.render(resolved.resolution().returnType()));
            case FunctionResolver.Ambiguous ambiguous -> Optional.of("ambiguous(" + ambiguous.candidates().size() + "): " + library.functions(name).size() + " overloads");
            case FunctionResolver.Incomplete _ -> Optional.of("incomplete");
            case FunctionResolver.NoMatch _ -> Optional.of("no-match: " + library.functions(name).size() + " overloads");
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
