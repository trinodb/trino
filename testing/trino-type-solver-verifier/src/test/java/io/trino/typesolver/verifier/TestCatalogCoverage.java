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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import org.junit.jupiter.api.Test;
import org.weakref.solver.TypeLibrary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Catalog-breadth dimension of the parity harness: measures how much of Trino's user-facing
 * scalar function catalog the typesolver preset models, and writes the gap to
 * {@code target/type-solver-parity/catalog-coverage-report.md}.
 * <p>
 * The assertion ratchets coverage: every unmodeled scalar builtin must appear in
 * {@link #DELIBERATELY_UNMODELED}. That allowlist documents the conscious exclusions, so a newly
 * unmodeled function — whether a regression that drops an existing one or a function added in a
 * future Trino bump — fails the test and forces a decision rather than silently eroding breadth.
 */
class TestCatalogCoverage
{
    /**
     * Scalar builtins intentionally not modeled:
     * <ul>
     *   <li>{@code bar}, {@code color}, {@code render}, {@code rgb} — ANSI-color visualization
     *       helpers over the cast-isolated {@code color} type, with bespoke {@code varchar(x)}
     *       width formulas; niche and not worth the modeling surface.</li>
     *   <li>{@code current_groups} — a {@code GROUPING}-context special form, resolved in the
     *       analyzer rather than the function registry (like {@code coalesce}/{@code position}).</li>
     * </ul>
     */
    private static final Set<String> DELIBERATELY_UNMODELED = Set.of("bar", "color", "render", "rgb", "current_groups");

    @Test
    void catalogCoverage()
            throws IOException
    {
        TestingFunctionResolution functions = new TestingFunctionResolution();
        TypeLibrary library = TrinoPreset.library();

        // User-facing scalar builtins: SCALAR kind, not hidden ($operator$/$literal$/etc.),
        // canonical names that don't start with '$'.
        TreeMap<String, String> trinoScalar = new TreeMap<>();
        for (FunctionMetadata metadata : functions.listGlobalFunctions()) {
            if (metadata.getKind() != FunctionKind.SCALAR || metadata.isHidden()) {
                continue;
            }
            for (String name : metadata.getNames()) {
                if (name.startsWith("$")) {
                    continue;
                }
                trinoScalar.merge(name, metadata.getSignature().toString(), (a, b) -> a + " | " + b);
            }
        }

        TreeSet<String> modeled = new TreeSet<>();
        TreeSet<String> unmodeled = new TreeSet<>();
        for (String name : trinoScalar.keySet()) {
            (library.functions(name).isEmpty() ? unmodeled : modeled).add(name);
        }

        StringBuilder out = new StringBuilder();
        out.append("# Catalog coverage report (Phase 1)\n\n");
        out.append("How much of Trino's user-facing scalar function catalog the typesolver preset models.\n\n");
        out.append("- Trino scalar builtins (distinct names): ").append(trinoScalar.size()).append('\n');
        out.append("- modeled by preset: ").append(modeled.size()).append('\n');
        out.append("- unmodeled: ").append(unmodeled.size()).append('\n');
        out.append("\n## Unmodeled (name -> Trino signature)\n\n");
        for (String name : unmodeled) {
            out.append("- `").append(name).append("` :: ").append(trinoScalar.get(name)).append('\n');
        }
        Path report = Path.of("target", "type-solver-parity", "catalog-coverage-report.md");
        Files.createDirectories(report.getParent());
        Files.writeString(report, out.toString());

        assertThat(unmodeled)
                .as("every unmodeled scalar builtin must be a documented, deliberate exclusion")
                .isSubsetOf(DELIBERATELY_UNMODELED);
    }
}
