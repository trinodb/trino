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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.CatalogFunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeTemplate;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import org.weakref.solver.TypeScheme;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;

/// Shadow-verifies whole-expression analysis against the type solver: every expression the
/// analyzer types is re-typed by [SolverExpressionTypeChecker], and the per-node types and
/// coercions must match the analyzer's exactly — a divergence throws, so any test or workload
/// run with the shadow enabled is a differential test of expression analysis, not just of
/// per-call function resolution.
///
/// Scope: scalar expression structure over builtin functions. A node whose form is outside the
/// checker — a column or other scoped reference, a subquery, an aggregate or window call, a row
/// subscript — takes the analyzer's computed type as given, so its subtree goes unverified while
/// everything around it still is; an expression that resolved through a non-builtin function is
/// skipped entirely, since the checker would bind its name against the wrong candidates. Either
/// way the shadow never rejects what the engine accepts for reasons other than disagreement.
/// Operators resolve under
/// the symbols the parser produces; the symmetric comparison forms share the canonical
/// operator's candidates, mirroring how the engine flips them onto one implementation.
///
/// Disabled by default; enabled with the `trino.solver-expression-shadow` system property or
/// programmatically for tests.
public final class SolverExpressionShadow
{
    private static final AtomicBoolean ENABLED = new AtomicBoolean(Boolean.getBoolean("trino.solver-expression-shadow"));

    private static final Map<String, OperatorType> OPERATOR_SYMBOLS = ImmutableMap.<String, OperatorType>builder()
            .put("+", OperatorType.ADD)
            .put("-", OperatorType.SUBTRACT)
            .put("*", OperatorType.MULTIPLY)
            .put("/", OperatorType.DIVIDE)
            .put("%", OperatorType.MODULO)
            .put("=", OperatorType.EQUAL)
            .put("<>", OperatorType.EQUAL)
            .put("<", OperatorType.LESS_THAN)
            .put(">", OperatorType.LESS_THAN)
            .put("<=", OperatorType.LESS_THAN_OR_EQUAL)
            .put(">=", OperatorType.LESS_THAN_OR_EQUAL)
            .put("[]", OperatorType.SUBSCRIPT)
            .buildOrThrow();

    private SolverExpressionShadow() {}

    public static boolean isEnabled()
    {
        return ENABLED.get();
    }

    @VisibleForTesting
    public static void setEnabled(boolean enabled)
    {
        ENABLED.set(enabled);
    }

    /// Verifies that the solver types the analyzed expression the way the engine did: same type
    /// on every node the checker visits, and the same coercion decision (or absence of one).
    public static void verifyAnalysis(
            Session session,
            PlannerContext plannerContext,
            Expression expression,
            Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions,
            Map<NodeRef<Expression>, Type> analyzerTypes,
            Map<NodeRef<Expression>, Type> analyzerCoercions)
    {
        // The checker resolves names against the builtin catalog only; a call the engine routed
        // elsewhere (an inline WITH FUNCTION, a path or catalog function) would falsely diverge
        for (ResolvedFunction function : resolvedFunctions.values()) {
            if (!isBuiltinFunctionName(function.signature().getName())) {
                return;
            }
        }

        // Forms outside the checker — column references, subqueries, aggregate calls — take the
        // analyzer's computed type as given: their subtrees go unverified, but every enclosing
        // call, special form, and coercion still is
        SolverExpressionTypeChecker.FunctionSchemes schemes = new SolverExpressionTypeChecker.FunctionSchemes()
        {
            @Override
            public List<TypeScheme> functions(String name)
            {
                return schemes(session, plannerContext.getMetadata(), name);
            }

            @Override
            public List<TypeScheme> instanceMethods(String name, String receiverBase)
            {
                return methodSchemes(session, plannerContext.getMetadata(), name, receiverBase, true);
            }

            @Override
            public List<TypeScheme> staticMethods(String name, String receiverBase)
            {
                return methodSchemes(session, plannerContext.getMetadata(), name, receiverBase, false);
            }
        };

        SolverExpressionTypeChecker checker = new SolverExpressionTypeChecker(
                SolverShadow.library().typeSystem(),
                SolverShadow.resolver(),
                schemes,
                plannerContext.getTypeManager(),
                node -> Optional.ofNullable(analyzerTypes.get(NodeRef.of(node))));

        SolverExpressionTypeChecker.Analysis analysis;
        try {
            analysis = checker.analyze(expression);
        }
        catch (UnsupportedOperationException _) {
            // A form, reference, or function the checker cannot express: out of scope
            return;
        }
        catch (SolverExpressionTypeChecker.TypeCheckFailure failure) {
            throw new VerifyException(
                    "Solver expression shadow divergence: solver rejects [%s] the engine accepted (%s): %s"
                            .formatted(expression, failure.kind(), failure.getMessage()));
        }

        analysis.types().forEach((node, type) -> {
            Type analyzerType = analyzerTypes.get(node);
            if (!type.equals(analyzerType)) {
                throw new VerifyException(
                        "Solver expression shadow divergence: [%s] in [%s] is %s in the engine but %s in the solver"
                                .formatted(node.getNode(), expression, analyzerType, type));
            }
            if (analysis.unverifiedCoercions().contains(node)) {
                // A descendant of an oracle-typed form: its coercion was decided by the
                // unresolved parent and cannot be re-derived
                return;
            }
            Type checkerCoercion = analysis.coercions().get(node);
            Type analyzerCoercion = analyzerCoercions.get(node);
            if (!Objects.equals(checkerCoercion, analyzerCoercion)) {
                throw new VerifyException(
                        "Solver expression shadow divergence: [%s] in [%s] coerces to %s in the engine but %s in the solver"
                                .formatted(node.getNode(), expression, analyzerCoercion, checkerCoercion));
            }
        });
    }

    /// The builtin candidate schemes of the name the checker resolves — a function name from the
    /// source, or an operator symbol the parser produced. Signals
    /// [UnsupportedOperationException] for anything it cannot fully express, which the checker's
    /// caller treats as "skip this expression".
    private static List<TypeScheme> schemes(Session session, Metadata metadata, String name)
    {
        String functionName = name;
        OperatorType operator = OPERATOR_SYMBOLS.get(name);
        if (operator != null) {
            functionName = mangleOperatorName(operator);
        }

        Collection<CatalogFunctionMetadata> candidates = metadata.getFunctions(session, builtinFunctionName(functionName));
        if (candidates.isEmpty()) {
            // Not a builtin: a path or catalog function, or a name only the analyzer knows
            throw new UnsupportedOperationException("Not a builtin function: " + name);
        }
        List<TypeScheme> schemes = new ArrayList<>(candidates.size());
        for (CatalogFunctionMetadata candidate : candidates) {
            FunctionMetadata candidateMetadata = candidate.functionMetadata();
            if (candidateMetadata.isMethod()) {
                // Methods (instance and static, i.e. anything declaring a receiver type) resolve
                // only through method-call syntax; exclude them as the engine's free-function
                // resolution does, which keeps only candidates with an empty receiver type
                continue;
            }
            if (candidateMetadata.getKind() != FunctionKind.SCALAR) {
                throw new UnsupportedOperationException("Not a scalar function: " + name);
            }
            schemes.add(SolverShadow.scheme(candidateMetadata)
                    .orElseThrow(() -> new UnsupportedOperationException("Signature not bridgeable: " + name)));
        }
        return schemes;
    }

    /// The method candidate schemes of `name` declared on `receiverBase` — instance methods when
    /// `instance`, static methods otherwise. Mirrors the engine's receiver-scoped method resolution:
    /// instance/static is selected by the receiver flag and the candidate's receiver base must match.
    /// Empty results are out of scope (the engine resolved against a candidate set this source cannot
    /// reproduce), signalled the same way an unbridgeable signature is.
    private static List<TypeScheme> methodSchemes(Session session, Metadata metadata, String name, String receiverBase, boolean instance)
    {
        List<TypeScheme> schemes = new ArrayList<>();
        for (CatalogFunctionMetadata candidate : metadata.getFunctions(session, builtinFunctionName(name))) {
            FunctionMetadata candidateMetadata = candidate.functionMetadata();
            if (candidateMetadata.isInstanceMethod() != instance
                    || candidateMetadata.getReceiverType().map(TypeTemplate::baseName).filter(receiverBase::equals).isEmpty()) {
                continue;
            }
            if (candidateMetadata.getKind() != FunctionKind.SCALAR) {
                throw new UnsupportedOperationException("Not a scalar method: " + name);
            }
            schemes.add(SolverShadow.scheme(candidateMetadata)
                    .orElseThrow(() -> new UnsupportedOperationException("Signature not bridgeable: " + name)));
        }
        if (schemes.isEmpty()) {
            throw new UnsupportedOperationException("No %s method '%s' on %s".formatted(instance ? "instance" : "static", name, receiverBase));
        }
        return schemes;
    }
}
