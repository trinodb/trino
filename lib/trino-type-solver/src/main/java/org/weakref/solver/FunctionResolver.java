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
package org.weakref.solver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Overload resolution — selects one signature from a list of candidates for a given call.
 * <p>
 * Each candidate is tried independently via {@link TypeScheme#matchFunctionCallOutcome}:
 * <ul>
 *   <li>{@link TypeScheme.Satisfied} results become {@link Resolution}s.</li>
 *   <li>{@link TypeScheme.Incomplete} results are remembered — the solver couldn't prove them
 *       infeasible but couldn't fully bind them either.</li>
 *   <li>{@link TypeScheme.Unsatisfied} results are discarded.</li>
 * </ul>
 * Outcome shape:
 * <ul>
 *   <li>No matches and no incompletes → {@link NoMatch}</li>
 *   <li>No matches but some incompletes → {@link Incomplete}</li>
 *   <li>One or more matches → {@link Specificity}-based dominance selects a unique winner
 *       ({@link Resolved}) or reports the surviving set ({@link Ambiguous}).</li>
 * </ul>
 * The framework's default {@link Specificity} is {@link Specificity#BY_COERCION_COUNT} — it
 * prefers candidates that required fewer coercions and otherwise declares candidates
 * incomparable. Dialect presets typically layer richer rules via {@link Specificity#then}.
 */
public final class FunctionResolver
{
    private final TypeSystem typeSystem;
    private final Specificity specificity;

    public FunctionResolver(TypeSystem typeSystem)
    {
        this(typeSystem, Specificity.BY_COERCION_COUNT);
    }

    public FunctionResolver(TypeSystem typeSystem, Specificity specificity)
    {
        this.typeSystem = typeSystem;
        this.specificity = specificity;
    }

    public ResolutionOutcome resolveOutcome(List<TypeScheme> candidates, List<Expression> arguments)
    {
        List<Resolution> matches = new ArrayList<>();
        List<TypeScheme> incomplete = new ArrayList<>();
        for (TypeScheme candidate : candidates) {
            switch (candidate.matchFunctionCallOutcome(arguments, typeSystem)) {
                case TypeScheme.Satisfied satisfied -> matches.add(toResolution(candidate, arguments, satisfied.result()));
                case TypeScheme.Incomplete _ -> incomplete.add(candidate);
                case TypeScheme.Unsatisfied _ -> {}
            }
        }

        if (matches.isEmpty()) {
            if (!incomplete.isEmpty()) {
                return new Incomplete(List.copyOf(incomplete));
            }
            return new NoMatch();
        }

        // Dominance: a candidate survives iff no other candidate is strictly MORE_SPECIFIC than it.
        List<Resolution> undominated = new ArrayList<>();
        for (int index = 0; index < matches.size(); index++) {
            Resolution current = matches.get(index);
            boolean dominated = false;
            for (int other = 0; other < matches.size(); other++) {
                if (other == index) {
                    continue;
                }
                if (specificity.compare(matches.get(other), current) == Specificity.Order.MORE_SPECIFIC) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                undominated.add(current);
            }
        }

        if (undominated.size() == 1) {
            return new Resolved(withCoercionPlans(undominated.getFirst()));
        }
        return new Ambiguous(undominated.stream().map(this::withCoercionPlans).toList());
    }

    private Resolution toResolution(TypeScheme scheme, List<Expression> arguments, TypeScheme.MatchResult match)
    {
        // Coercion plans are left null here and filled in by {@link #withCoercionPlans} only for the
        // candidates that survive dominance — specificity compares on types alone.
        List<ArgumentCoercion> coercions = new ArrayList<>();
        for (int index = 0; index < arguments.size(); index++) {
            Expression actual = arguments.get(index);
            Expression template = match.parameterTemplates().get(index);
            Expression formal = match.parameterTypes().get(index);
            coercions.add(new ArgumentCoercion(
                    index,
                    actual,
                    formal,
                    template,
                    TypeScheme.coercionNeeded(actual, template, formal, match.solverResult()),
                    null));
        }
        return new Resolution(
                scheme,
                match.returnType(),
                List.copyOf(coercions),
                match.typeBindings(),
                match.numericBindings(),
                match.solverResult());
    }

    private Resolution withCoercionPlans(Resolution resolution)
    {
        List<ArgumentCoercion> coercions = resolution.argumentCoercions().stream()
                .map(coercion -> new ArgumentCoercion(
                        coercion.index(),
                        coercion.actualType(),
                        coercion.formalType(),
                        coercion.template(),
                        coercion.coercionNeeded(),
                        TypeScheme.coercionPlanFor(coercion.actualType(), coercion.template(), coercion.formalType(), resolution.solverResult(), typeSystem)))
                .toList();
        return new Resolution(
                resolution.scheme(),
                resolution.returnType(),
                coercions,
                resolution.typeBindings(),
                resolution.numericBindings(),
                resolution.solverResult());
    }

    /**
     * Successful binding of a call to a specific candidate.
     *
     * @param scheme the winning scheme
     * @param returnType the fully-resolved return type of the call
     * @param argumentCoercions per-argument coercion plan (one entry per actual argument)
     * @param typeBindings map from the scheme's quantified type vars to bound types
     * @param numericBindings map from the scheme's numeric vars to concrete integer values
     * @param solverResult the underlying solver result — exposes bounds and pending
     *         constraints for advanced inspection
     */
    public record Resolution(
            TypeScheme scheme,
            Expression returnType,
            List<ArgumentCoercion> argumentCoercions,
            Map<String, Expression> typeBindings,
            Map<String, Integer> numericBindings,
            Solver.Result solverResult) {}

    /**
     * Result of a call resolution attempt. See {@link FunctionResolver} class doc for which
     * variant is produced when.
     */
    public sealed interface ResolutionOutcome
            permits Ambiguous,
                    Incomplete,
                    NoMatch,
                    Resolved {}

    /**
     * Exactly one candidate was chosen.
     */
    public record Resolved(Resolution resolution)
            implements ResolutionOutcome {}

    /**
     * No candidate was applicable.
     */
    public record NoMatch()
            implements ResolutionOutcome {}

    /**
     * No candidate bound fully, but at least one couldn't be ruled out either — the solver
     * left unresolved variables. The listed candidates remain "nearly matching" for diagnostics.
     */
    public record Incomplete(List<TypeScheme> candidates)
            implements ResolutionOutcome {}

    /**
     * Multiple candidates survived dominance — none is strictly more specific than the others
     * under the configured {@link Specificity}. The caller should report this as ambiguous.
     */
    public record Ambiguous(List<Resolution> candidates)
            implements ResolutionOutcome {}

    /**
     * Per-argument record of how the actual argument was (or wasn't) converted to match the
     * formal parameter. A pure data record — classification is no longer bundled here; if
     * a caller needs to know whether a conversion is primitive widening, structural, etc.,
     * it can inspect {@link CoercionPlan#kind()} and the plan's step IDs directly.
     */
    public record ArgumentCoercion(
            int index,
            Expression actualType,
            Expression formalType,
            Expression template,
            boolean coercionNeeded,
            CoercionPlan coercionPlan) {}
}
