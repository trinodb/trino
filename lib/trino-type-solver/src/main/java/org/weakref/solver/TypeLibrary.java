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

import org.weakref.solver.type.TypeConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Embedder-facing facade bundling a {@link TypeSystem}, a set of named function signatures,
 * and a {@link FunctionResolver} tuned with a {@link Specificity}.
 * <p>
 * A library is immutable — build it once with {@link #builder()} and query repeatedly. The
 * typical lifecycle in an embedding engine is:
 * <ol>
 *   <li>Register type constructors, coercion rules, cast rules, and function signatures.</li>
 *   <li>Call {@link #resolveFunction} during query analysis to bind calls to overloads.</li>
 *   <li>Use the returned {@link FunctionResolver.Resolution} (argument coercions, return
 *       type, type bindings) as planner input.</li>
 * </ol>
 * For direct constraint programming outside of function calls, {@link #solve} and
 * {@link #solveOutcome} expose the underlying solver, and {@link #resolveCast} queries
 * the registered cast rules.
 */
public final class TypeLibrary
{
    private final TypeSystem typeSystem;
    private final FunctionResolver functionResolver;
    private final Map<String, List<TypeScheme>> functions;

    private TypeLibrary(TypeSystem typeSystem, Specificity specificity, Map<String, List<TypeScheme>> functions)
    {
        this.typeSystem = typeSystem;
        this.functionResolver = new FunctionResolver(typeSystem, specificity);
        this.functions = functions;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public TypeSystem typeSystem()
    {
        return typeSystem;
    }

    /**
     * All registered signatures for the given function name. Returns empty list if unknown.
     */
    public List<TypeScheme> functions(String name)
    {
        return functions.getOrDefault(name, List.of());
    }

    /**
     * Resolve a call {@code name(arguments...)} against the registered overloads. The result
     * is one of {@link FunctionResolver.Resolved}, {@link FunctionResolver.Ambiguous},
     * {@link FunctionResolver.Incomplete}, or {@link FunctionResolver.NoMatch}.
     */
    public FunctionResolver.ResolutionOutcome resolveFunction(String name, List<Expression> arguments)
    {
        return functionResolver.resolveOutcome(functions(name), arguments);
    }

    /**
     * Return a {@link CoercionPlan} for an explicit cast from {@code from} to {@code to},
     * considering both implicit coercions and cast-only rules. Empty if no rule applies.
     */
    public Optional<CoercionPlan> resolveCast(Expression from, Expression to)
    {
        return typeSystem.castPlan(from, to);
    }

    public Solver.SolveOutcome solveOutcome(List<Constraint> constraints)
    {
        return new Solver(typeSystem).solveOutcome(constraints);
    }

    public Solver.Result solve(List<Constraint> constraints)
    {
        return new Solver(typeSystem).solve(constraints);
    }

    /**
     * Fluent builder. All {@code register*} methods append to ordered lists — registration
     * order is preserved and occasionally significant (e.g. {@link SelfCoercion} should be
     * registered first so trivial matches short-circuit the rule scan).
     */
    public static final class Builder
    {
        private final List<TypeConstructor> types = new ArrayList<>();
        private final List<CoercionRule> coercions = new ArrayList<>();
        private final List<CoercionRule> castRules = new ArrayList<>();
        private final Map<String, List<TypeScheme>> functions = new LinkedHashMap<>();
        private Specificity specificity = Specificity.BY_COERCION_COUNT;

        public Builder registerType(TypeConstructor type)
        {
            types.add(type);
            return this;
        }

        public Builder registerCoercion(CoercionRule coercion)
        {
            coercions.add(coercion);
            return this;
        }

        public Builder registerCast(CoercionRule castRule)
        {
            castRules.add(castRule);
            return this;
        }

        public Builder registerFunction(String name, TypeScheme scheme)
        {
            functions.computeIfAbsent(name, _ -> new ArrayList<>()).add(scheme);
            return this;
        }

        public Builder registerFunction(String name, Expression type)
        {
            return registerFunction(name, new TypeScheme(List.of(), List.of(), type));
        }

        /**
         * Set the {@link Specificity} used to break ties among applicable overloads.
         * Defaults to {@link Specificity#BY_COERCION_COUNT}. Dialect presets typically
         * replace this (or layer on top via {@link Specificity#then}).
         */
        public Builder withSpecificity(Specificity specificity)
        {
            this.specificity = specificity;
            return this;
        }

        public TypeLibrary build()
        {
            TypeSystem typeSystem = new TypeSystem(List.copyOf(types), List.copyOf(coercions), List.copyOf(castRules));
            Map<String, List<TypeScheme>> registeredFunctions = new LinkedHashMap<>();
            functions.forEach((name, schemes) -> registeredFunctions.put(name, List.copyOf(schemes)));
            return new TypeLibrary(typeSystem, specificity, Map.copyOf(registeredFunctions));
        }
    }
}
