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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Embedder-facing facade bundling a [TypeSystem], a set of named function signatures,
/// and a [FunctionResolver] tuned with a [Specificity].
///
/// A library is immutable — build it once with [#builder()] and query repeatedly. The
/// typical lifecycle in an embedding engine is:
///
/// 1. Register type constructors, coercion rules, cast rules, and function signatures.
/// 2. Call [#resolveFunction] during query analysis to bind calls to overloads.
/// 3. Use the returned [FunctionResolver.Resolution] (argument coercions, return
///    type, type bindings) as planner input.
///
/// For direct constraint programming outside of function calls, [#solve] and
/// [#solveOutcome] expose the underlying solver, and [#resolveCast] queries
/// the registered cast rules.
public final class TypeLibrary
{
    private final TypeSystem typeSystem;
    private final FunctionResolver functionResolver;
    private final Map<String, List<CompiledSignature>> functions;
    private final Map<MethodKey, List<CompiledSignature>> methods;

    /// Keys the method tables by name, the receiver's base type, and whether the method is an
    /// instance method — the dimensions the engine scopes method resolution by. An instance method
    /// carries the receiver as its leading formal; a static method's formals are the call arguments.
    private record MethodKey(String name, String receiverBase, boolean instance) {}

    private TypeLibrary(TypeSystem typeSystem, Specificity specificity, Map<String, List<CompiledSignature>> functions, Map<MethodKey, List<CompiledSignature>> methods)
    {
        this.typeSystem = typeSystem;
        this.functionResolver = new FunctionResolver(typeSystem, specificity);
        this.functions = functions;
        this.methods = methods;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public TypeSystem typeSystem()
    {
        return typeSystem;
    }

    /// The resolver [#resolveFunction] consults — exposed so callers can drive the same
    /// resolution (same specificity ranking) over candidate sets not registered in this library.
    public FunctionResolver resolver()
    {
        return functionResolver;
    }

    /// All registered signatures for the given function name. Returns empty list if unknown.
    public List<CompiledSignature> functions(String name)
    {
        return functions.getOrDefault(name, List.of());
    }

    /// Resolve a call `name(arguments...)` against the registered overloads. The result
    /// is one of [FunctionResolver.Resolved], [FunctionResolver.Ambiguous],
    /// [FunctionResolver.Incomplete], or [FunctionResolver.NoMatch].
    public FunctionResolver.ResolutionOutcome resolveFunction(String name, List<Expression> arguments)
    {
        return functionResolver.resolveOutcome(functions(name), arguments);
    }

    /// All registered instance methods named `name` on the given receiver base type. Each
    /// scheme's first formal is the receiver (self). Returns empty list if none.
    public List<CompiledSignature> instanceMethods(String name, String receiverBase)
    {
        return methods.getOrDefault(new MethodKey(name, receiverBase, true), List.of());
    }

    /// All registered static methods named `name` on the given receiver base type. The
    /// receiver type is a selector only — the schemes' formals are the call arguments. Empty if none.
    public List<CompiledSignature> staticMethods(String name, String receiverBase)
    {
        return methods.getOrDefault(new MethodKey(name, receiverBase, false), List.of());
    }

    /// Resolve an instance method call `receiver.name(arguments...)` against the instance
    /// methods on the receiver's base type. The arguments lead with the receiver (self), matching
    /// each candidate's first formal.
    public FunctionResolver.ResolutionOutcome resolveInstanceMethod(String name, String receiverBase, List<Expression> arguments)
    {
        return functionResolver.resolveOutcome(instanceMethods(name, receiverBase), arguments);
    }

    /// Resolve a static method call `Type::name(arguments...)` against the static methods on
    /// the named type's base. The receiver type is a selector only; the arguments are the call's
    /// arguments.
    public FunctionResolver.ResolutionOutcome resolveStaticMethod(String name, String receiverBase, List<Expression> arguments)
    {
        return functionResolver.resolveOutcome(staticMethods(name, receiverBase), arguments);
    }

    /// Return a [CoercionPlan] for an explicit cast from `from` to `to`,
    /// considering both implicit coercions and cast-only rules. Empty if no rule applies.
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

    /// Fluent builder. All `register*` methods append to ordered lists — registration
    /// order is preserved and occasionally significant (e.g. [SelfCoercion] should be
    /// registered first so trivial matches short-circuit the rule scan).
    public static final class Builder
    {
        private final List<TypeConstructor> types = new ArrayList<>();
        private final List<CoercionRule> coercions = new ArrayList<>();
        private final List<CoercionRule> castRules = new ArrayList<>();
        private final Map<String, List<CompiledSignature>> functions = new LinkedHashMap<>();
        private final Map<MethodKey, List<CompiledSignature>> methods = new LinkedHashMap<>();
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

        public Builder registerFunction(String name, CompiledSignature scheme)
        {
            functions.computeIfAbsent(name, _ -> new ArrayList<>()).add(scheme);
            return this;
        }

        public Builder registerFunction(String name, Signature signature)
        {
            return registerFunction(name, SignatureCompiler.compile(signature));
        }

        public Builder registerFunction(String name, Expression type)
        {
            return registerFunction(name, new CompiledSignature(List.of(), List.of(), type));
        }

        /// Register an instance method `name` on `receiverBase`. The scheme's first
        /// formal is the receiver (self); the rest are the call's arguments.
        public Builder registerInstanceMethod(String name, String receiverBase, CompiledSignature scheme)
        {
            methods.computeIfAbsent(new MethodKey(name, receiverBase, true), _ -> new ArrayList<>()).add(scheme);
            return this;
        }

        public Builder registerInstanceMethod(String name, String receiverBase, Signature signature)
        {
            return registerInstanceMethod(name, receiverBase, SignatureCompiler.compile(signature));
        }

        public Builder registerInstanceMethod(String name, String receiverBase, Expression type)
        {
            return registerInstanceMethod(name, receiverBase, new CompiledSignature(List.of(), List.of(), type));
        }

        /// Register a static method `name` on `receiverBase`. The receiver type is a
        /// selector only; the scheme's formals are exactly the call's arguments.
        public Builder registerStaticMethod(String name, String receiverBase, CompiledSignature scheme)
        {
            methods.computeIfAbsent(new MethodKey(name, receiverBase, false), _ -> new ArrayList<>()).add(scheme);
            return this;
        }

        public Builder registerStaticMethod(String name, String receiverBase, Signature signature)
        {
            return registerStaticMethod(name, receiverBase, SignatureCompiler.compile(signature));
        }

        public Builder registerStaticMethod(String name, String receiverBase, Expression type)
        {
            return registerStaticMethod(name, receiverBase, new CompiledSignature(List.of(), List.of(), type));
        }

        /// Set the [Specificity] used to break ties among applicable overloads.
        /// Defaults to [Specificity#BY_COERCION_COUNT]. Dialect presets typically
        /// replace this (or layer on top via [Specificity#then]).
        public Builder withSpecificity(Specificity specificity)
        {
            this.specificity = specificity;
            return this;
        }

        public TypeLibrary build()
        {
            TypeSystem typeSystem = new TypeSystem(List.copyOf(types), List.copyOf(coercions), List.copyOf(castRules));
            Map<String, List<CompiledSignature>> registeredFunctions = new LinkedHashMap<>();
            functions.forEach((name, schemes) -> registeredFunctions.put(name, List.copyOf(schemes)));
            Map<MethodKey, List<CompiledSignature>> registeredMethods = new LinkedHashMap<>();
            methods.forEach((key, schemes) -> registeredMethods.put(key, List.copyOf(schemes)));
            return new TypeLibrary(typeSystem, specificity, Map.copyOf(registeredFunctions), Map.copyOf(registeredMethods));
        }
    }
}
