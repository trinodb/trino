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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.type.TypeCoercion;
import org.junit.jupiter.api.Test;
import org.weakref.solver.Expression;
import org.weakref.solver.Solver;
import org.weakref.solver.Subtype;
import org.weakref.solver.TypeSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 differential harness: drives the same concrete type pairs through the
 * solver's Trino preset and through Trino's real {@link TypeCoercion}, then
 * records where the two engines disagree.
 * <p>
 * The test is a measurement, not a gate: it asserts only that the harness ran
 * and that the type bridge is total over the sample. Every disagreement is
 * written to {@code target/type-solver-parity/parity-report.md} as the Phase 1
 * deliverable; closing those gaps is downstream work.
 */
class TestTypeSolverParity
{
    // A representative slice of Trino's type universe: scalars, parametric
    // numerics/strings, the temporal precision families, and a few structural
    // nestings. Expressed as signature strings so both engines build from one source.
    private static final List<String> SAMPLE_SIGNATURES = List.of(
            "boolean",
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
            "varchar",
            "varchar(5)",
            "varchar(50)",
            "char(3)",
            "char(10)",
            "varbinary",
            "date",
            "time(3)",
            "time(6)",
            "time(6) with time zone",
            "timestamp(0)",
            "timestamp(3)",
            "timestamp(6)",
            "timestamp(3) with time zone",
            "timestamp(6) with time zone",
            "interval year to month",
            "interval day to second",
            "json",
            "uuid",
            "ipaddress",
            "variant",
            "HyperLogLog",
            "P4HyperLogLog",
            "tdigest",
            "qdigest(bigint)",
            "SetDigest",
            "color",
            "json2016",
            "unknown",
            "decimal(1,0)",
            "decimal(20,10)",
            "decimal(38,38)",
            "varchar(1)",
            "char(1)",
            "char(255)",
            "time(0)",
            "time(12)",
            "time(0) with time zone",
            "timestamp(9)",
            "timestamp(12)",
            "timestamp(0) with time zone",
            "array(integer)",
            "array(bigint)",
            "array(varchar(5))",
            "array(decimal(10,2))",
            "map(varchar, bigint)",
            "map(integer, varchar(5))",
            "map(varchar(5), array(bigint))",
            "row(integer, varchar(3))",
            "row(bigint, double)",
            "array(array(integer))",
            "array(array(array(integer)))",
            "array(row(integer, varchar(3)))",
            "row(array(bigint), map(varchar(5), decimal(10,2)))");

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final TypeManager typeManager = functionResolution.getPlannerContext().getTypeManager();
    private final TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType, SQL_STANDARD);
    private final TypeSystem solver = TrinoPreset.typeSystem();
    private final Solver engine = new Solver(solver);

    @Test
    void differentialParity()
            throws IOException
    {
        List<Sample> samples = SAMPLE_SIGNATURES.stream()
                .map(signature -> {
                    Type trinoType = typeManager.getType(parseTypeDescriptor(signature));
                    return new Sample(signature, trinoType, TypeBridge.toExpression(trinoType));
                })
                .toList();

        List<Divergence> coercionDivergences = new ArrayList<>();
        List<Divergence> castDivergences = new ArrayList<>();
        List<Divergence> supertypeDivergences = new ArrayList<>();
        List<Divergence> guardGaps = new ArrayList<>();
        int coercionAgree = 0;
        int castAgree = 0;
        int supertypeAgree = 0;
        int pairs = 0;

        for (Sample from : samples) {
            for (Sample to : samples) {
                pairs++;

                // Implicit coercibility: Trino's canCoerce vs the solver's full engine
                // (solveOutcome over a Subtype constraint), which evaluates numeric guards.
                String trinoCoerce = evaluate(() -> Boolean.toString(typeCoercion.canCoerce(from.trinoType(), to.trinoType())));
                String solverCoerce = evaluate(() -> Boolean.toString(
                        engine.solveOutcome(List.of(new Subtype(from.expression(), to.expression()))) instanceof Solver.Satisfied));
                if (trinoCoerce.equals(solverCoerce)) {
                    coercionAgree++;
                }
                else {
                    coercionDivergences.add(new Divergence(from.signature(), to.signature(), trinoCoerce, solverCoerce));
                }

                // Internal solver consistency: the lightweight coercionPlan() does not
                // evaluate NumericRelation guards for ground types, so it disagrees with
                // the full engine on guarded narrowing. Surface that gap on its own.
                String planCoerce = evaluate(() -> Boolean.toString(solver.coercionPlan(from.expression(), to.expression()).isPresent()));
                if (!planCoerce.equals(solverCoerce)) {
                    guardGaps.add(new Divergence(from.signature(), to.signature(), solverCoerce, planCoerce));
                }

                // Explicit cast existence: Trino's getCoercion (the CAST operator, throwing when
                // no cast exists) vs the solver's castPlan (implicit + cast-only rules).
                boolean trinoCast = trinoCastExists(from.trinoType(), to.trinoType());
                boolean solverCast = solver.castPlan(from.expression(), to.expression()).isPresent();
                if (trinoCast == solverCast) {
                    castAgree++;
                }
                else {
                    castDivergences.add(new Divergence(from.signature(), to.signature(), Boolean.toString(trinoCast), Boolean.toString(solverCast)));
                }

                String trinoSuper = evaluate(() -> commonSupertype(typeCoercion.getCommonSuperType(from.trinoType(), to.trinoType())
                        .map(TypeBridge::toExpression)));
                String solverSuper = evaluate(() -> commonSupertype(solver.getCommonSupertype(from.expression(), to.expression())));
                if (trinoSuper.equals(solverSuper)) {
                    supertypeAgree++;
                }
                else {
                    supertypeDivergences.add(new Divergence(from.signature(), to.signature(), trinoSuper, solverSuper));
                }
            }
        }

        Path report = Path.of("target", "type-solver-parity", "parity-report.md");
        Files.createDirectories(report.getParent());
        Files.writeString(report, render(samples.size(), pairs, coercionAgree, coercionDivergences, castAgree, castDivergences, supertypeAgree, supertypeDivergences, guardGaps));

        // The harness ran and the bridge mapped every sample type; parity itself is reported, not asserted.
        assertThat(samples).hasSameSizeAs(SAMPLE_SIGNATURES);
        assertThat(pairs).isEqualTo(samples.size() * samples.size());
        assertThat(report).exists();
    }

    private boolean trinoCastExists(Type from, Type to)
    {
        try {
            functionResolution.getCoercion(from, to);
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private static String commonSupertype(Optional<Expression> result)
    {
        return result.map(TypeBridge::render).orElse("<none>");
    }

    private static String evaluate(ResultSupplier supplier)
    {
        try {
            return supplier.get();
        }
        catch (RuntimeException e) {
            return "ERROR: " + e.getClass().getSimpleName();
        }
    }

    private static String render(
            int typeCount,
            int pairs,
            int coercionAgree,
            List<Divergence> coercionDivergences,
            int castAgree,
            List<Divergence> castDivergences,
            int supertypeAgree,
            List<Divergence> supertypeDivergences,
            List<Divergence> guardGaps)
    {
        StringBuilder out = new StringBuilder();
        out.append("# Type-solver parity report (Phase 1)\n\n");
        out.append("Differential comparison of the typesolver Trino preset against Trino's own `TypeCoercion`.\n");
        out.append("Implicit coercion is taken from the solver's full engine (`Solver.solveOutcome` over a `Subtype` constraint); ");
        out.append("explicit-cast existence from `TypeSystem.castPlan` vs Trino's `Metadata.getCoercion`.\n\n");
        out.append("- Sample types: ").append(typeCount).append('\n');
        out.append("- Ordered pairs: ").append(pairs).append("\n\n");
        out.append("| Question | Agree | Disagree | Agreement |\n");
        out.append("|---|---|---|---|\n");
        appendScoreRow(out, "implicit coercion (engine `solveOutcome`)", coercionAgree, coercionDivergences.size(), pairs);
        appendScoreRow(out, "explicit cast existence", castAgree, castDivergences.size(), pairs);
        appendScoreRow(out, "common supertype", supertypeAgree, supertypeDivergences.size(), pairs);
        out.append('\n');
        appendDivergences(out, "Implicit-coercion disagreements", "from", "to", "Trino canCoerce", "solver engine", coercionDivergences);
        appendDivergences(out, "Explicit-cast disagreements", "from", "to", "Trino cast", "solver cast", castDivergences);
        appendDivergences(out, "Common-supertype disagreements", "a", "b", "Trino supertype", "solver supertype", supertypeDivergences);
        out.append("## Solver-internal: `coercionPlan` guard-evaluation gap (").append(guardGaps.size()).append(")\n\n");
        out.append("Pairs where the lightweight `TypeSystem.coercionPlan` disagrees with the full engine. ");
        out.append("`coercionPlan` matches a rule structurally but does not evaluate its `NumericRelation` ");
        out.append("guards for ground types, so it admits guarded narrowing the engine rejects. ");
        out.append("This is an internal solver inconsistency to fix upstream, independent of Trino parity.\n\n");
        appendTable(out, "from", "to", "engine", "coercionPlan", guardGaps);
        return out.toString();
    }

    private static void appendScoreRow(StringBuilder out, String label, int agree, int disagree, int pairs)
    {
        out.append("| ").append(label).append(" | ").append(agree).append(" | ").append(disagree)
                .append(" | ").append(String.format("%.1f%%", 100.0 * agree / pairs)).append(" |\n");
    }

    private static void appendDivergences(StringBuilder out, String title, String leftHeader, String rightHeader, String trinoHeader, String solverHeader, List<Divergence> divergences)
    {
        out.append("## ").append(title).append(" (").append(divergences.size()).append(")\n\n");
        appendTable(out, leftHeader, rightHeader, trinoHeader, solverHeader, divergences);
    }

    private static final int MAX_ROWS = 120;

    private static void appendTable(StringBuilder out, String leftHeader, String rightHeader, String trinoHeader, String solverHeader, List<Divergence> divergences)
    {
        if (divergences.isEmpty()) {
            out.append("_None._\n\n");
            return;
        }
        out.append("| ").append(leftHeader).append(" | ").append(rightHeader).append(" | ").append(trinoHeader).append(" | ").append(solverHeader).append(" |\n");
        out.append("|---|---|---|---|\n");
        for (Divergence divergence : divergences.stream().limit(MAX_ROWS).toList()) {
            out.append("| `").append(divergence.left()).append("` | `").append(divergence.right()).append("` | ")
                    .append(divergence.trino()).append(" | ").append(divergence.solver()).append(" |\n");
        }
        if (divergences.size() > MAX_ROWS) {
            out.append("\n_… and ").append(divergences.size() - MAX_ROWS).append(" more._\n");
        }
        out.append('\n');
    }

    private interface ResultSupplier
    {
        String get();
    }

    private record Sample(String signature, Type trinoType, Expression expression) {}

    private record Divergence(String left, String right, String trino, String solver) {}
}
