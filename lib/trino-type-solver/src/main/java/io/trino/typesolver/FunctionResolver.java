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
package io.trino.typesolver;

import io.trino.spi.function.Signature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/// Overload resolution — selects one signature from a list of candidates for a given call.
///
/// Each candidate is tried independently via [CompiledSignature#matchFunctionCallOutcome]:
///
/// - [CompiledSignature.Satisfied] results become [Resolution]s.
/// - [CompiledSignature.Incomplete] results are remembered — the solver couldn't prove them
///   infeasible but couldn't fully bind them either.
/// - [CompiledSignature.Unsatisfied] results are discarded.
///
/// Outcome shape:
///
/// - No matches and no incompletes → [NoMatch]
/// - No matches but some incompletes → [Incomplete]
/// - One or more matches → [Specificity]-based dominance selects a unique winner
///   ([Resolved]) or reports the surviving set ([Ambiguous]).
///
/// The framework's default [Specificity] is [Specificity#BY_COERCION_COUNT] — it
/// prefers candidates that required fewer coercions and otherwise declares candidates
/// incomparable. Dialect presets typically layer richer rules via [Specificity#then].
public final class FunctionResolver
{
    private final TypeSystem typeSystem;
    private final Specificity specificity;
    private final Map<Signature, CompiledSignature> compiledSignatures = new ConcurrentHashMap<>();

    public FunctionResolver(TypeSystem typeSystem)
    {
        this(typeSystem, Specificity.BY_COERCION_COUNT);
    }

    public FunctionResolver(TypeSystem typeSystem, Specificity specificity)
    {
        this.typeSystem = typeSystem;
        this.specificity = specificity;
    }

    public ResolutionOutcome resolveOutcome(List<CompiledSignature> candidates, List<Expression> arguments)
    {
        return resolveNamed(candidates, arguments.stream().map(Argument::positional).toList());
    }

    /// Resolve Trino function signatures directly. [Signature] remains the declaration and
    /// [CompiledSignature] is only the solver's compiled constraint form.
    public ResolutionOutcome resolveSignatures(List<Signature> candidates, List<Expression> arguments)
    {
        return resolveSignaturesNamed(candidates, arguments.stream().map(Argument::positional).toList());
    }

    /// Resolve named arguments against Trino function signatures directly.
    public ResolutionOutcome resolveSignaturesNamed(List<Signature> candidates, List<Argument> arguments)
    {
        return ResolutionBudget.withBudget(() -> resolveCandidates(candidates.stream()
                .map(signature -> new Candidate(
                        Optional.of(signature),
                        compiledSignatures.computeIfAbsent(signature, SignatureCompiler::compile)))
                .toList(), arguments));
    }

    /// Resolve a call whose actuals may carry argument names. Each candidate maps the named actuals
    /// onto its own declared parameter positions and is then matched positionally, so name
    /// resolution and type-based overload selection are one pass over the candidates — a candidate
    /// that does not declare a supplied name simply does not match. The returned coercions are
    /// indexed by the written argument position, not the signature position, so callers need not
    /// know the permutation.
    public ResolutionOutcome resolveNamed(List<CompiledSignature> candidates, List<Argument> arguments)
    {
        return ResolutionBudget.withBudget(() -> resolveNamedInternal(candidates, arguments));
    }

    private ResolutionOutcome resolveNamedInternal(List<CompiledSignature> candidates, List<Argument> arguments)
    {
        return resolveCandidates(candidates.stream()
                .map(candidate -> new Candidate(Optional.empty(), candidate))
                .toList(), arguments);
    }

    private ResolutionOutcome resolveCandidates(List<Candidate> candidates, List<Argument> arguments)
    {
        List<Resolution> matches = new ArrayList<>();
        List<CompiledSignature> incomplete = new ArrayList<>();
        for (Candidate candidate : candidates) {
            CompiledSignature compiled = candidate.compiled();
            Optional<int[]> permutation = argumentPermutation(arguments, compiled.argumentNames());
            if (permutation.isEmpty()) {
                continue;
            }
            int[] writtenForPosition = permutation.get();
            List<Expression> ordered = new ArrayList<>(writtenForPosition.length);
            for (int position : writtenForPosition) {
                ordered.add(arguments.get(position).type());
            }
            switch (compiled.matchFunctionCallOutcome(ordered, typeSystem)) {
                case CompiledSignature.Satisfied satisfied -> matches.add(toResolution(candidate, ordered, writtenForPosition, satisfied.result()));
                case CompiledSignature.Incomplete _ -> incomplete.add(compiled);
                case CompiledSignature.Unsatisfied _ -> {}
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

    private Resolution toResolution(Candidate candidate, List<Expression> arguments, int[] writtenForPosition, CompiledSignature.MatchResult match)
    {
        // Coercion plans are left null here and filled in by {@link #withCoercionPlans} only for the
        // candidates that survive dominance — specificity compares on types alone. The coercion's
        // index is the written argument position (a named call permutes the actuals into signature
        // order to match, but the caller records coercions against the positions it passed).
        List<ArgumentCoercion> coercions = new ArrayList<>();
        for (int index = 0; index < arguments.size(); index++) {
            Expression actual = arguments.get(index);
            Expression template = match.parameterTemplates().get(index);
            Expression formal = match.parameterTypes().get(index);
            coercions.add(new ArgumentCoercion(
                    writtenForPosition[index],
                    actual,
                    formal,
                    template,
                    CompiledSignature.coercionNeeded(actual, template, formal, match.solverResult()),
                    null));
        }
        return new Resolution(
                candidate.signature(),
                candidate.compiled(),
                match.returnType(),
                List.copyOf(coercions),
                match.typeBindings(),
                match.numericBindings(),
                match.solverResult());
    }

    /// The written-argument index that fills each signature position, or empty if this scheme
    /// cannot accept the call. A purely positional call (no names) maps identically. A named call
    /// requires the scheme to declare exactly this arity with names: positional actuals keep their
    /// leading positions, each named actual goes to the position its name occupies, and a name the
    /// scheme does not declare — or one that collides with a position already filled — disqualifies
    /// the scheme. Variadic schemes never declare enough names to satisfy a named call, so named
    /// arguments and variadic functions are mutually exclusive without special-casing.
    private static Optional<int[]> argumentPermutation(List<Argument> arguments, List<Optional<String>> parameterNames)
    {
        int arity = arguments.size();
        int firstNamed = arity;
        for (int index = 0; index < arity; index++) {
            if (arguments.get(index).name().isPresent()) {
                firstNamed = index;
                break;
            }
        }
        if (firstNamed == arity) {
            // Positional call: identity, regardless of whether the scheme declares names
            int[] identity = new int[arity];
            for (int index = 0; index < arity; index++) {
                identity[index] = index;
            }
            return Optional.of(identity);
        }
        // Named resolution needs one declared name per position at exactly this arity
        if (parameterNames.size() != arity) {
            return Optional.empty();
        }
        int[] writtenForPosition = new int[arity];
        Arrays.fill(writtenForPosition, -1);
        for (int index = 0; index < firstNamed; index++) {
            writtenForPosition[index] = index;
        }
        for (int index = firstNamed; index < arity; index++) {
            String name = arguments.get(index).name().orElseThrow();
            int position = -1;
            for (int candidate = firstNamed; candidate < arity; candidate++) {
                if (parameterNames.get(candidate).filter(name::equals).isPresent()) {
                    position = candidate;
                    break;
                }
            }
            if (position < 0 || writtenForPosition[position] != -1) {
                return Optional.empty();
            }
            writtenForPosition[position] = index;
        }
        for (int value : writtenForPosition) {
            if (value == -1) {
                return Optional.empty();
            }
        }
        return Optional.of(writtenForPosition);
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
                        CompiledSignature.coercionPlanFor(coercion.actualType(), coercion.template(), coercion.formalType(), resolution.solverResult(), typeSystem)))
                .toList();
        return new Resolution(
                resolution.signature(),
                resolution.compiledSignature(),
                resolution.returnType(),
                coercions,
                resolution.typeBindings(),
                resolution.numericBindings(),
                resolution.solverResult());
    }

    /// Successful binding of a call to a specific candidate.
    ///
    /// @param signature the authoritative Trino declaration, when resolution started from Trino signatures
    /// @param compiledSignature the winning compiled signature
    /// @param returnType the fully-resolved return type of the call
    /// @param argumentCoercions per-argument coercion plan (one entry per actual argument)
    /// @param typeBindings map from the scheme's quantified type vars to bound types
    /// @param numericBindings map from the scheme's numeric vars to concrete integer values
    /// @param solverResult the underlying solver result — exposes bounds and pending
    ///         constraints for advanced inspection
    public record Resolution(
            Optional<Signature> signature,
            CompiledSignature compiledSignature,
            Expression returnType,
            List<ArgumentCoercion> argumentCoercions,
            Map<String, Expression> typeBindings,
            Map<String, Integer> numericBindings,
            Solver.Result solverResult) {}

    private record Candidate(Optional<Signature> signature, CompiledSignature compiled) {}

    /// Result of a call resolution attempt. See [FunctionResolver] class doc for which
    /// variant is produced when.
    public sealed interface ResolutionOutcome
            permits Ambiguous,
                    Incomplete,
                    NoMatch,
                    Resolved {}

    /// Exactly one candidate was chosen.
    public record Resolved(Resolution resolution)
            implements ResolutionOutcome {}

    /// No candidate was applicable.
    public record NoMatch()
            implements ResolutionOutcome {}

    /// No candidate bound fully, but at least one couldn't be ruled out either — the solver
    /// left unresolved variables. The listed candidates remain "nearly matching" for diagnostics.
    public record Incomplete(List<CompiledSignature> candidates)
            implements ResolutionOutcome {}

    /// Multiple candidates survived dominance — none is strictly more specific than the others
    /// under the configured [Specificity]. The caller should report this as ambiguous.
    public record Ambiguous(List<Resolution> candidates)
            implements ResolutionOutcome {}

    /// Per-argument record of how the actual argument was (or wasn't) converted to match the
    /// formal parameter. A pure data record — classification is no longer bundled here; if
    /// a caller needs to know whether a conversion is primitive widening, structural, etc.,
    /// it can inspect [CoercionPlan#kind()] and the plan's step IDs directly.
    public record ArgumentCoercion(
            int index,
            Expression actualType,
            Expression formalType,
            Expression template,
            boolean coercionNeeded,
            CoercionPlan coercionPlan) {}

    /// One actual argument of a call: its type, and the parameter name it was written under for a
    /// named argument (`f(x => 1)`), or empty for a positional one.
    public record Argument(Optional<String> name, Expression type)
    {
        public static Argument positional(Expression type)
        {
            return new Argument(Optional.empty(), type);
        }

        public static Argument named(String name, Expression type)
        {
            return new Argument(Optional.of(name), type);
        }
    }
}
