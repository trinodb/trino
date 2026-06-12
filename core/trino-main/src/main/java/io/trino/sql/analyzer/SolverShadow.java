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
package io.trino.sql.analyzer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import io.trino.Session;
import io.trino.lib.SignatureBridge;
import io.trino.lib.TrinoPreset;
import io.trino.lib.TrinoSpecificity;
import io.trino.lib.TypeBridge;
import io.trino.lib.UnboundedVarcharSentinelCoercion;
import io.trino.metadata.CatalogFunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.type.Type;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.Specificity;
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeScheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;

/// Shadow-verifies the engine's function resolution against the type solver: given the same actual
/// argument types, the solver must choose the same candidate and produce the same bound signature
/// (return type and formal parameter types) the SignatureBinder-based resolution produced. A
/// divergence throws, so any test or workload run with the shadow enabled is a differential test of
/// the solver against the production resolution path.
///
/// Scope: builtin scalar functions whose signatures bridge to solver schemes. Calls outside that
/// scope — connector or inline functions, non-scalar kinds, signatures the bridge cannot express —
/// are skipped, never failed: the shadow must not reject what the engine accepts for reasons other
/// than disagreement. A lambda argument is verified at its resolved function type (the deferred
/// analysis already typed it against the chosen formal), so the shadow checks candidate choice and
/// signature materialization, not lambda parameter-type inference.
///
/// Disabled by default; enabled with the `trino.solver-shadow` system property or
/// programmatically for tests. Schemes are bridged once per [FunctionId] and memoized.
public final class SolverShadow
{
    private static final AtomicBoolean ENABLED = new AtomicBoolean(Boolean.getBoolean("trino.solver-shadow"));
    private static final Map<FunctionId, Optional<TypeScheme>> SCHEMES = new ConcurrentHashMap<>();
    private static final Supplier<TypeLibrary> LIBRARY = Suppliers.memoize(SolverShadow::createLibrary);
    private static final Supplier<FunctionResolver> RESOLVER = Suppliers.memoize(SolverShadow::createResolver);

    private SolverShadow() {}

    public static boolean isEnabled()
    {
        return ENABLED.get();
    }

    @VisibleForTesting
    public static void setEnabled(boolean enabled)
    {
        ENABLED.set(enabled);
    }

    /// Verifies that the solver resolves the call the way the engine did. The argument types are
    /// the pre-coercion actuals, with lambdas at their resolved function types.
    public static void verifyResolution(Session session, Metadata metadata, ResolvedFunction function, List<Type> actualArgumentTypes)
    {
        BoundSignature signature = function.signature();
        CatalogSchemaFunctionName name = signature.getName();
        if (!isBuiltinFunctionName(name)) {
            return;
        }

        List<TypeScheme> candidates = new ArrayList<>();
        for (CatalogFunctionMetadata candidate : metadata.getFunctions(session, name)) {
            FunctionMetadata candidateMetadata = candidate.functionMetadata();
            if (candidateMetadata.isMethod()) {
                // Methods (instance and static, i.e. anything declaring a receiver type) resolve
                // only through method-call syntax; the engine's free-function resolution keeps just
                // candidates with an empty receiver type, so they are not candidates here
                continue;
            }
            if (candidateMetadata.getKind() != FunctionKind.SCALAR) {
                // mixed or non-scalar name: out of the shadow's scope
                return;
            }
            Optional<TypeScheme> scheme = scheme(candidateMetadata);
            if (scheme.isEmpty()) {
                // A candidate the bridge cannot express could have changed the resolution
                // outcome; verifying against a partial candidate set would report false
                // divergences, so skip the call entirely
                return;
            }
            candidates.add(scheme.get());
        }

        List<Expression> arguments;
        try {
            arguments = actualArgumentTypes.stream()
                    .map(TypeBridge::toExpression)
                    .toList();
        }
        catch (RuntimeException _) {
            // An actual type the bridge cannot express is out of scope, same as an
            // unbridgeable candidate signature
            return;
        }

        switch (RESOLVER.get().resolveOutcome(candidates, arguments)) {
            case FunctionResolver.Resolved resolved -> verifySignature(name, arguments, signature, resolved.resolution());
            case FunctionResolver.Ambiguous ambiguous -> {
                // The engine treats candidates that agree on the return type as semantically the
                // same and picks one arbitrarily (FunctionBinder's unknown-argument tie-break), so
                // an ambiguity whose candidates all return what the engine returned is agreement
                String expected = render(signature);
                if (ambiguous.candidates().stream()
                        .allMatch(candidate -> TypeBridge.render(candidate.returnType()).equals(expected))) {
                    return;
                }
                throw new VerifyException(
                        "Solver shadow divergence: '%s'%s resolved by the engine to %s, but the solver finds %s ambiguous candidates"
                                .formatted(name, arguments, expected, ambiguous.candidates().size()));
            }
            case FunctionResolver.Incomplete incomplete -> throw new VerifyException(
                    "Solver shadow divergence: '%s'%s resolved by the engine to %s, but the solver leaves it unresolved (near misses: %s)"
                            .formatted(name, arguments, render(signature), incomplete.candidates()));
            case FunctionResolver.NoMatch _ -> throw new VerifyException(
                    "Solver shadow divergence: '%s'%s resolved by the engine to %s, but the solver finds no match"
                            .formatted(name, arguments, render(signature)));
        }
    }

    private static void verifySignature(CatalogSchemaFunctionName name, List<Expression> arguments, BoundSignature signature, FunctionResolver.Resolution resolution)
    {
        String expectedReturn = TypeBridge.render(TypeBridge.toExpression(signature.getReturnType()));
        String actualReturn = TypeBridge.render(resolution.returnType());
        if (!expectedReturn.equals(actualReturn)) {
            throw new VerifyException(
                    "Solver shadow divergence: '%s'%s returns %s in the engine but %s in the solver"
                            .formatted(name, arguments, expectedReturn, actualReturn));
        }
        for (FunctionResolver.ArgumentCoercion coercion : resolution.argumentCoercions()) {
            String expectedFormal = TypeBridge.render(TypeBridge.toExpression(signature.getArgumentTypes().get(coercion.index())));
            String actualFormal = TypeBridge.render(coercion.formalType());
            if (!expectedFormal.equals(actualFormal)) {
                throw new VerifyException(
                        "Solver shadow divergence: '%s'%s parameter %s is %s in the engine but %s in the solver"
                                .formatted(name, arguments, coercion.index(), expectedFormal, actualFormal));
            }
        }
    }

    private static String render(BoundSignature signature)
    {
        return TypeBridge.render(TypeBridge.toExpression(signature.getReturnType()));
    }

    /// The function's scheme, bridged once per [FunctionId] and memoized; empty when the bridge
    /// cannot express the signature.
    static Optional<TypeScheme> scheme(FunctionMetadata metadata)
    {
        return SCHEMES.computeIfAbsent(metadata.getFunctionId(), _ -> bridge(metadata));
    }

    /// The shared rule library — types, coercions, casts, no functions (candidates come from
    /// catalog metadata) — the shadow components resolve against.
    static TypeLibrary library()
    {
        return LIBRARY.get();
    }

    /// The shared resolver, ranking candidates the way the engine's FunctionBinder does.
    static FunctionResolver resolver()
    {
        return RESOLVER.get();
    }

    private static Optional<TypeScheme> bridge(FunctionMetadata metadata)
    {
        try {
            return Optional.of(SignatureBridge.toTypeScheme(metadata.getSignature()));
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static TypeLibrary createLibrary()
    {
        TypeLibrary.Builder builder = TypeLibrary.builder();
        TrinoPreset.typeConstructors().forEach(builder::registerType);
        TrinoPreset.coercionRules().forEach(builder::registerCoercion);
        TrinoPreset.castRules().forEach(builder::registerCast);
        // Catalog signatures are parametric over varchar(x) with the MAX_VALUE sentinel standing
        // in for unbounded — re-encoded exactly, not coerced, so overload selection matches the
        // engine's preference for varchar forms over genuinely converting candidates
        builder.registerCoercion(new UnboundedVarcharSentinelCoercion());
        return builder.build();
    }

    private static FunctionResolver createResolver()
    {
        return new FunctionResolver(library().typeSystem(), Specificity.BY_COERCION_COUNT.then(new TrinoSpecificity(library().typeSystem())));
    }
}
