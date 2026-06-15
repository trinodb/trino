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

import io.trino.lib.TypeBridge;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.WhenClause;
import org.weakref.solver.Constraint;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.RequireComparable;
import org.weakref.solver.RequireOrderable;
import org.weakref.solver.TypeScheme;
import org.weakref.solver.TypeSystem;
import org.weakref.solver.VariableAllocator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.variable;

/// Solver-based expression type checking: types a scalar expression tree against a solver
/// [TypeSystem] and [FunctionSchemes] catalog instead of the bespoke `ExpressionAnalyzer`.
///
/// Each call is one constraint problem. A lambda argument enters the solve as a function type over
/// fresh variables — there is no eager/deferred argument split (no `TypeDescriptorProvider`): the
/// lambda's parameter types fall out of unifying the *other* arguments against the candidate's
/// scheme. The one irreducible analyzer obligation — typing the lambda *body* once its parameter
/// types are determined — is discharged between the probing solve and the final solve, which then
/// runs with all arguments ground so full overload dominance applies.
public final class SolverExpressionTypeChecker
{
    /// Supplies the candidate schemes of a function name — a registered library, or a live
    /// catalog lookup. Implementations signal a name they cannot fully express (a candidate
    /// outside the bridgeable surface) with [UnsupportedOperationException], which callers
    /// treat as "this expression is out of the checker's scope", never as a rejection.
    public interface FunctionSchemes
    {
        List<TypeScheme> functions(String name);
    }

    private final TypeSystem typeSystem;
    private final FunctionResolver resolver;
    private final FunctionSchemes functions;
    private final TypeManager typeManager;
    private final Function<io.trino.sql.tree.Expression, Optional<Type>> leafTypes;
    private final VariableAllocator allocator = new VariableAllocator();
    private final Map<NodeRef<io.trino.sql.tree.Expression>, Type> types = new HashMap<>();
    private final Map<NodeRef<io.trino.sql.tree.Expression>, Type> coercions = new HashMap<>();
    private final Set<NodeRef<io.trino.sql.tree.Expression>> unverifiedCoercions = new HashSet<>();

    // Resolution caches, shared across analyze() calls when enabled — the analog of the engine's
    // BuiltinFunctionResolver caches: a call's outcome and a lambda call's parameter assignments
    // are determined by the name and the ground argument shapes, so they memoize cleanly over a
    // fixed library. Null when caching is off (the per-expression shadow runs a fresh checker, so
    // there is nothing to reuse); the value carries between expressions only when a long-lived
    // checker holds it.
    private final Map<ResolutionKey, FunctionResolver.ResolutionOutcome> resolutionCache;
    private final Map<ProbeKey, Set<List<List<Expression>>>> probeCache;

    public SolverExpressionTypeChecker(TypeSystem typeSystem, FunctionResolver resolver, FunctionSchemes functions, TypeManager typeManager)
    {
        this(typeSystem, resolver, functions, typeManager, _ -> Optional.empty());
    }

    public SolverExpressionTypeChecker(
            TypeSystem typeSystem,
            FunctionResolver resolver,
            FunctionSchemes functions,
            TypeManager typeManager,
            Function<io.trino.sql.tree.Expression, Optional<Type>> leafTypes)
    {
        this(typeSystem, resolver, functions, typeManager, leafTypes, false);
    }

    /// The leaf-type oracle: when a node's form is outside the checker (a column reference, a
    /// subquery, an aggregate call), the node's type is taken from the oracle as given — its
    /// subtree goes unverified, but every enclosing call, special form, and coercion around it
    /// still is. Without an oracle entry the unsupported form propagates and the whole expression
    /// is out of scope.
    ///
    /// With `cacheResolutions`, a long-lived checker memoizes call outcomes and lambda parameter
    /// assignments across expressions, the way the engine's resolver caches bound functions — the
    /// warm steady state. Leave it off for one-shot use, where the caches only add overhead.
    public SolverExpressionTypeChecker(
            TypeSystem typeSystem,
            FunctionResolver resolver,
            FunctionSchemes functions,
            TypeManager typeManager,
            Function<io.trino.sql.tree.Expression, Optional<Type>> leafTypes,
            boolean cacheResolutions)
    {
        this.typeSystem = requireNonNull(typeSystem, "typeSystem is null");
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.functions = requireNonNull(functions, "functions is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.leafTypes = requireNonNull(leafTypes, "leafTypes is null");
        this.resolutionCache = cacheResolutions ? new HashMap<>() : null;
        this.probeCache = cacheResolutions ? new HashMap<>() : null;
        // Keep the checker's fresh variables visibly distinct from scheme-instantiation variables
        allocator.reserveThrough(1_000_000);
    }

    /// Caches a call outcome on the name and its (named) argument list.
    private record ResolutionKey(String name, List<FunctionResolver.Argument> arguments) {}

    /// Caches a lambda probe on the name and its ground argument shapes — the lambda placeholder
    /// arguments a probe passes are normalized to their arity, since the fresh variables a probe
    /// mints differ from one analysis to the next but the assignment they yield does not.
    private record ProbeKey(String name, List<Expression> arguments) {}

    private FunctionResolver.ResolutionOutcome resolveOutcome(String name, List<FunctionResolver.Argument> arguments)
    {
        if (resolutionCache == null) {
            return resolver.resolveNamed(functions.functions(name), arguments);
        }
        return resolutionCache.computeIfAbsent(
                new ResolutionKey(name, arguments),
                key -> resolver.resolveNamed(functions.functions(key.name()), key.arguments()));
    }

    private static List<FunctionResolver.Argument> positionalArguments(List<Expression> arguments)
    {
        return arguments.stream().map(FunctionResolver.Argument::positional).toList();
    }

    public Type typeOf(io.trino.sql.tree.Expression expression)
    {
        return analyze(expression).type();
    }

    /// Types the expression tree and returns the full analyzer-shaped product: a type for every
    /// visited node and the coercions the resolution decided — the same maps `ExpressionAnalyzer`
    /// hands to the planner.
    public Analysis analyze(io.trino.sql.tree.Expression expression)
    {
        types.clear();
        coercions.clear();
        unverifiedCoercions.clear();
        Type type = typeOf(expression, Map.of());
        return new Analysis(type, Map.copyOf(types), Map.copyOf(coercions), Set.copyOf(unverifiedCoercions));
    }

    /// The `unverifiedCoercions` set holds the descendants of oracle-typed nodes: a parent the
    /// checker could not resolve decides its children's coercions, so those decisions cannot be
    /// re-derived — the nodes' types still verify, their coercion entries do not.
    public record Analysis(
            Type type,
            Map<NodeRef<io.trino.sql.tree.Expression>, Type> types,
            Map<NodeRef<io.trino.sql.tree.Expression>, Type> coercions,
            Set<NodeRef<io.trino.sql.tree.Expression>> unverifiedCoercions) {}

    /// A rejected expression, categorized — the checker's counterpart of the analyzer's
    /// `TYPE_MISMATCH` / `FUNCTION_NOT_FOUND` semantic exceptions, so negative-path differential
    /// tests can compare rejection categories and not just the fact of rejection.
    public static final class TypeCheckFailure
            extends RuntimeException
    {
        public enum Kind
        {
            /// No candidate of the named function matched the arguments (or no such function)
            NO_MATCH,
            /// Multiple candidates survived dominance, or lambda assignments disagree on the result
            AMBIGUOUS,
            /// Branches of a CASE/COALESCE/IF/NULLIF/IN/BETWEEN share no common supertype
            NO_COMMON_TYPE,
            /// A syntactic position requiring boolean (condition, logical operand) held another type
            BOOLEAN_REQUIRED,
        }

        private final Kind kind;

        TypeCheckFailure(Kind kind, String message)
        {
            super(message);
            this.kind = requireNonNull(kind, "kind is null");
        }

        public Kind kind()
        {
            return kind;
        }
    }

    private Type typeOf(io.trino.sql.tree.Expression node, Map<String, Type> scope)
    {
        Type type;
        try {
            type = uncachedTypeOf(node, scope);
        }
        catch (UnsupportedOperationException e) {
            // A form the checker cannot derive: take the supplied type when the oracle has one,
            // so the surrounding expression still verifies; otherwise stay out of scope. The
            // unresolved form decided its descendants' coercions (an aggregate coerces its
            // arguments the way any call does), so those entries become unverifiable
            type = leafTypes.apply(node).orElseThrow(() -> e);
            markDescendantCoercionsUnverified(node);
        }
        types.put(NodeRef.of(node), type);
        return type;
    }

    private void markDescendantCoercionsUnverified(Node node)
    {
        for (Node child : node.getChildren()) {
            if (child instanceof io.trino.sql.tree.Expression expression) {
                unverifiedCoercions.add(NodeRef.of(expression));
            }
            markDescendantCoercionsUnverified(child);
        }
    }

    private Type uncachedTypeOf(io.trino.sql.tree.Expression node, Map<String, Type> scope)
    {
        return switch (node) {
            case LongLiteral literal -> literal.getParsedValue() == (int) literal.getParsedValue() ? INTEGER : BIGINT;
            case DecimalLiteral literal -> Decimals.parse(literal.getValue()).getType();
            case DoubleLiteral _ -> DOUBLE;
            case BooleanLiteral _ -> BOOLEAN;
            // varchar length counts code points, not UTF-16 units, matching the engine's literal typing
            case StringLiteral literal -> createVarcharType(literal.getValue().codePointCount(0, literal.getValue().length()));
            case Identifier identifier -> {
                Type bound = scope.get(identifier.getValue());
                if (bound == null) {
                    // Not a lambda parameter: a column or other scoped reference the checker
                    // does not resolve — out of scope, not a rejection
                    throw new UnsupportedOperationException("Unsupported identifier: " + identifier.getValue());
                }
                yield bound;
            }
            case Cast cast -> typeManager.getType(TypeDescriptorTranslator.toTypeDescriptor(cast.getType()));
            case NullLiteral _ -> UNKNOWN;
            case ArithmeticBinaryExpression operation -> resolvePositionalCall(operation.getOperator().getValue(), List.of(operation.getLeft(), operation.getRight()), scope);
            case Predicated predicated -> {
                // The LHS now rides on the Predicated wrapper; each fragment reproduces the
                // constraint shape its former standalone node carried
                io.trino.sql.tree.Expression value = predicated.getValue();
                yield switch (predicated.getPredicate()) {
                    case ComparisonPredicate comparison -> resolvePositionalCall(comparison.getOperator().getValue(), List.of(value, comparison.getRight()), scope);
                    case IsNullPredicate _ -> {
                        // IS [NOT] NULL is a boolean of any argument; the negated flag is irrelevant to typing
                        typeOf(value, scope);
                        yield BOOLEAN;
                    }
                    case BetweenPredicate between -> {
                        superType(List.of(value, between.getMin(), between.getMax()), scope, List.of(new RequireOrderable("@t")), true);
                        yield BOOLEAN;
                    }
                    case InPredicate in -> {
                        if (!(in.getValueList() instanceof InListExpression list)) {
                            throw new UnsupportedOperationException("Unsupported IN form: " + in);
                        }
                        List<io.trino.sql.tree.Expression> values = new ArrayList<>();
                        values.add(value);
                        values.addAll(list.getValues());
                        superType(values, scope, List.of(new RequireComparable("@t")), true);
                        yield BOOLEAN;
                    }
                    default -> throw new UnsupportedOperationException("Unsupported predicate: " + predicated.getPredicate());
                };
            }
            case FunctionCall call -> {
                // A qualified name is a catalog/schema-qualified call or a method invocation on a
                // receiver — resolution the checker does not model
                if (call.getName().getPrefix().isPresent()) {
                    throw new UnsupportedOperationException("Unsupported qualified call: " + call.getName());
                }
                yield resolveCall(
                        call.getName().getSuffix(),
                        call.getArguments().stream().map(CallArgument::getValue).toList(),
                        call.getArguments().stream().map(argument -> argument.getName().map(Identifier::getValue)).toList(),
                        scope);
            }
            case SubscriptExpression subscript -> {
                // Row subscript is field access, special-cased in the analyzer — only the
                // array/map subscript operator is a resolvable call
                if (typeOf(subscript.getBase(), scope) instanceof RowType) {
                    throw new UnsupportedOperationException("Unsupported subscript on a row type: " + subscript);
                }
                yield resolvePositionalCall("[]", List.of(subscript.getBase(), subscript.getIndex()), scope);
            }
            // The constructs below are special-cased in ExpressionAnalyzer; here each is the same
            // constraint shape a generic signature produces — branches flowing into one type
            // variable — plus the boolean obligations the syntax carries
            case LogicalExpression logical -> {
                logical.getTerms().forEach(term -> requireBoolean(term, scope, "Logical operand"));
                yield BOOLEAN;
            }
            case NotExpression not -> {
                requireBoolean(not.getValue(), scope, "NOT operand");
                yield BOOLEAN;
            }
            case IfExpression ifExpression -> {
                requireBoolean(ifExpression.getCondition(), scope, "IF condition");
                List<io.trino.sql.tree.Expression> branches = new ArrayList<>();
                branches.add(ifExpression.getTrueValue());
                ifExpression.getFalseValue().ifPresent(branches::add);
                yield superType(branches, scope, List.of(), true);
            }
            case NullIfExpression nullIf -> {
                // The arguments must share a common type, but the result keeps the first
                // argument's type and no coercion is recorded — exactly the analyzer's treatment
                superType(List.of(nullIf.getFirst(), nullIf.getSecond()), scope, List.of(), false);
                yield typeOf(nullIf.getFirst(), scope);
            }
            case CoalesceExpression coalesce -> superType(coalesce.getOperands(), scope, List.of(), true);
            case SearchedCaseExpression caseExpression -> {
                caseExpression.getWhenClauses().forEach(when -> requireBoolean(caseOperand(when), scope, "CASE condition"));
                yield superType(caseResults(caseExpression.getWhenClauses(), caseExpression.getDefaultValue()), scope, List.of(), true);
            }
            case SimpleCaseExpression caseExpression -> {
                List<io.trino.sql.tree.Expression> compared = new ArrayList<>();
                compared.add(caseExpression.getOperand());
                caseExpression.getWhenClauses().forEach(when -> compared.add(caseOperand(when)));
                superType(compared, scope, List.of(new RequireComparable("@t")), true);
                yield superType(caseResults(caseExpression.getWhenClauses(), caseExpression.getDefaultValue()), scope, List.of(), true);
            }
            case Row row -> {
                Expression.RowField[] fields = row.getFields().stream()
                        .map(field -> {
                            Expression type = TypeBridge.toExpression(typeOf(field.getExpression(), scope));
                            // Unquoted identifiers canonicalize, exactly as the analyzer names row fields
                            return field.getName()
                                    .map(name -> Expression.field(name.getCanonicalValue(), type))
                                    .orElseGet(() -> Expression.anonymousField(type));
                        })
                        .toArray(Expression.RowField[]::new);
                yield toType(Expression.row(fields));
            }
            default -> throw new UnsupportedOperationException("Unsupported expression: " + node.getClass().getSimpleName());
        };
    }

    private static List<io.trino.sql.tree.Expression> caseResults(List<WhenClause> whenClauses, Optional<io.trino.sql.tree.Expression> defaultValue)
    {
        List<io.trino.sql.tree.Expression> results = new ArrayList<>();
        whenClauses.forEach(when -> results.add(when.getResult()));
        defaultValue.ifPresent(results::add);
        return results;
    }

    private static io.trino.sql.tree.Expression caseOperand(WhenClause when)
    {
        return switch (when.getMatch()) {
            case WhenClause.Operand operand -> operand.expression();
            case WhenClause.Partial _ -> throw new UnsupportedOperationException("Unsupported CASE predicate: " + when);
        };
    }

    private void requireBoolean(io.trino.sql.tree.Expression node, Map<String, Type> scope, String role)
    {
        Type type = typeOf(node, scope);
        if (type.equals(BOOLEAN)) {
            return;
        }
        // The analyzer coerces a boolean-position operand when it can (a null's unknown type),
        // recording the coercion — only an incoercible type is a rejection
        if (typeSystem.coercionPlan(TypeBridge.toExpression(type), Expression.symbol("boolean")).isPresent()) {
            coercions.put(NodeRef.of(node), BOOLEAN);
            return;
        }
        throw new TypeCheckFailure(TypeCheckFailure.Kind.BOOLEAN_REQUIRED, "%s must be boolean, found %s: %s".formatted(role, type, node));
    }

    /// The least common supertype of the given expressions' types: every branch flows into a single
    /// type variable — the same constraint shape a generic `(T, ..., T) -> T` signature produces —
    /// so the solver's coercion search finds the result type instead of a bespoke pairwise
    /// `getCommonSuperType` walk. With `recordCoercions`, every branch not already at the common
    /// type gets a coercion to it, the way the analyzer's `coerceToSingleType` records them.
    private Type superType(List<io.trino.sql.tree.Expression> nodes, Map<String, Type> scope, List<Constraint> constraints, boolean recordCoercions)
    {
        List<Type> branchTypes = nodes.stream()
                .map(node -> typeOf(node, scope))
                .toList();
        List<Expression> arguments = branchTypes.stream()
                .map(TypeBridge::toExpression)
                .toList();
        TypeScheme scheme = new TypeScheme(
                List.of(variable("@t")),
                constraints,
                function(List.copyOf(nCopies(arguments.size(), variable("@t"))), variable("@t")));
        if (!(scheme.matchFunctionCallOutcome(arguments, typeSystem) instanceof TypeScheme.Satisfied satisfied)) {
            throw new TypeCheckFailure(TypeCheckFailure.Kind.NO_COMMON_TYPE, "No common supertype for %s".formatted(arguments));
        }
        Type common = toType(satisfied.result().returnType());
        if (recordCoercions) {
            for (int branch = 0; branch < nodes.size(); branch++) {
                if (!branchTypes.get(branch).equals(common)) {
                    coercions.put(NodeRef.of(nodes.get(branch)), common);
                }
            }
        }
        return common;
    }

    private record PendingLambda(int index, LambdaExpression lambda, List<String> parameterVariables) {}

    private Type resolvePositionalCall(String name, List<io.trino.sql.tree.Expression> argumentNodes, Map<String, Type> scope)
    {
        return resolveCall(name, argumentNodes, nCopies(argumentNodes.size(), Optional.<String>empty()), scope);
    }

    private Type resolveCall(String name, List<io.trino.sql.tree.Expression> argumentNodes, List<Optional<String>> argumentNames, Map<String, Type> scope)
    {
        List<Expression> arguments = new ArrayList<>();
        List<PendingLambda> pendingLambdas = new ArrayList<>();
        for (int index = 0; index < argumentNodes.size(); index++) {
            if (argumentNodes.get(index) instanceof LambdaExpression lambda) {
                List<String> parameterVariables = lambda.getArguments().stream()
                        .map(_ -> allocator.newVariable())
                        .toList();
                arguments.add(function(
                        parameterVariables.stream().map(Expression::variable).map(Expression.class::cast).toList(),
                        variable(allocator.newVariable())));
                pendingLambdas.add(new PendingLambda(index, lambda, parameterVariables));
            }
            else {
                arguments.add(TypeBridge.toExpression(typeOf(argumentNodes.get(index), scope)));
            }
        }

        if (pendingLambdas.isEmpty()) {
            List<FunctionResolver.Argument> namedArguments = new ArrayList<>(arguments.size());
            for (int index = 0; index < arguments.size(); index++) {
                namedArguments.add(new FunctionResolver.Argument(argumentNames.get(index), arguments.get(index)));
            }
            return resolveRecordingCoercions(name, argumentNodes, namedArguments);
        }

        if (argumentNames.stream().anyMatch(Optional::isPresent)) {
            // Determining a lambda's parameter types depends on the overload, and named arguments
            // depend on the overload too; resolving both at once is out of scope for now
            throw new UnsupportedOperationException("Named arguments with lambda arguments are unsupported: " + name);
        }

        // Probing solve, per candidate: with the lambdas as function types over fresh variables,
        // unifying the other arguments against a candidate's scheme determines each lambda formal's
        // parameter types. Different candidates may assign different parameter types, under which a
        // lambda body may type differently — so the obligation is discharged per distinct assignment,
        // and each completed assignment gets a final solve over ground arguments with full dominance.
        Set<List<List<Expression>>> assignments = probe(name, arguments, pendingLambdas.stream().map(PendingLambda::index).toList());

        Set<Type> results = new LinkedHashSet<>();
        for (List<List<Expression>> assignment : assignments) {
            // The probe's assignment is the candidate's minimal binding; the final resolution can
            // widen a shared variable (reduce's state flows through the lambda's return back into
            // its parameters), and the engine types the lambda at the widened formal — so
            // re-discharge the bodies at the resolved formal's parameter types until stable
            List<Expression> completed;
            for (int attempt = 0; ; attempt++) {
                completed = withLambdasDischarged(arguments, pendingLambdas, assignment, scope);
                List<List<Expression>> refined = refineAssignment(name, completed, pendingLambdas, assignment);
                if (refined.equals(assignment)) {
                    break;
                }
                if (attempt >= 4) {
                    throw new UnsupportedOperationException("Lambda parameter types do not converge for '%s'".formatted(name));
                }
                assignment = refined;
            }
            // Final solve with every argument ground: full overload dominance applies. With
            // several surviving assignments the last one's coercions win; the ambiguity check
            // below rejects the call unless they agree on the result anyway
            results.add(resolveRecordingCoercions(name, argumentNodes, positionalArguments(completed)));
        }

        if (results.size() != 1) {
            throw new TypeCheckFailure(TypeCheckFailure.Kind.AMBIGUOUS, "Call to '%s' is ambiguous: lambda arguments type differently under different candidates: %s".formatted(name, results));
        }
        return results.iterator().next();
    }

    /// Types each lambda body with the assignment's parameter types in scope — the analyzer
    /// obligation — and completes the argument list with the resulting ground function types.
    /// Bookkeeping under a body is cleared first: a re-discharge at widened parameter types must
    /// not leave entries from the narrower pass behind.
    private List<Expression> withLambdasDischarged(
            List<Expression> arguments,
            List<PendingLambda> pendingLambdas,
            List<List<Expression>> assignment,
            Map<String, Type> scope)
    {
        List<Expression> completed = new ArrayList<>(arguments);
        for (int lambdaOrdinal = 0; lambdaOrdinal < pendingLambdas.size(); lambdaOrdinal++) {
            PendingLambda pending = pendingLambdas.get(lambdaOrdinal);
            List<Expression> parameterTypes = assignment.get(lambdaOrdinal);
            Map<String, Type> lambdaScope = new HashMap<>(scope);
            for (int parameter = 0; parameter < parameterTypes.size(); parameter++) {
                lambdaScope.put(pending.lambda().getArguments().get(parameter).getName().getValue(), toType(parameterTypes.get(parameter)));
            }
            clearBookkeeping(pending.lambda().getBody());
            Type bodyType = typeOf(pending.lambda().getBody(), lambdaScope);
            completed.set(pending.index(), function(parameterTypes, TypeBridge.toExpression(bodyType)));
        }
        return completed;
    }

    /// The lambda parameter types the final resolution actually settled on. When resolution
    /// widens them past the probe's assignment, the engine would have typed the lambda at the
    /// wider formal — the caller re-discharges until this is a fixpoint.
    private List<List<Expression>> refineAssignment(String name, List<Expression> completed, List<PendingLambda> pendingLambdas, List<List<Expression>> assignment)
    {
        if (!(resolveOutcome(name, positionalArguments(completed)) instanceof FunctionResolver.Resolved resolved)) {
            return assignment;
        }
        Map<Integer, Expression> formals = new HashMap<>();
        for (FunctionResolver.ArgumentCoercion coercion : resolved.resolution().argumentCoercions()) {
            formals.put(coercion.index(), coercion.formalType());
        }
        List<List<Expression>> refined = new ArrayList<>();
        for (int lambdaOrdinal = 0; lambdaOrdinal < pendingLambdas.size(); lambdaOrdinal++) {
            Expression formal = formals.get(pendingLambdas.get(lambdaOrdinal).index());
            if (formal instanceof Expression.FunctionType functionType && functionType.parameterTypes().stream().allMatch(Expression::isGround)) {
                refined.add(List.copyOf(functionType.parameterTypes()));
            }
            else {
                refined.add(assignment.get(lambdaOrdinal));
            }
        }
        return refined;
    }

    private void clearBookkeeping(Node node)
    {
        if (node instanceof io.trino.sql.tree.Expression expression) {
            NodeRef<io.trino.sql.tree.Expression> reference = NodeRef.of(expression);
            types.remove(reference);
            coercions.remove(reference);
            unverifiedCoercions.remove(reference);
        }
        for (Node child : node.getChildren()) {
            clearBookkeeping(child);
        }
    }

    /// Probes every candidate and returns the distinct ground parameter-type assignments for the
    /// lambda arguments: one entry per assignment, holding the parameter types of each lambda in
    /// order. Candidates that do not determine all lambda parameter types are skipped.
    private Set<List<List<Expression>>> probe(String name, List<Expression> arguments, List<Integer> lambdaIndexes)
    {
        if (probeCache == null) {
            return uncachedProbe(name, arguments, lambdaIndexes);
        }
        // The lambda placeholder arguments hold freshly minted variables that differ from one
        // analysis to the next; normalize them to their arity so the key is shape-stable
        List<Expression> normalized = arguments.stream()
                .map(argument -> argument instanceof Expression.FunctionType functionType && functionType.parameterTypes().stream().allMatch(Expression.Variable.class::isInstance)
                        ? Expression.apply("__lambda", Expression.literal(functionType.parameterTypes().size()))
                        : argument)
                .toList();
        return probeCache.computeIfAbsent(
                new ProbeKey(name, normalized),
                _ -> uncachedProbe(name, arguments, lambdaIndexes));
    }

    private Set<List<List<Expression>>> uncachedProbe(String name, List<Expression> arguments, List<Integer> lambdaIndexes)
    {
        Set<List<List<Expression>>> assignments = new LinkedHashSet<>();
        for (TypeScheme candidate : functions.functions(name)) {
            if (!(candidate.matchFunctionCallOutcome(arguments, typeSystem) instanceof TypeScheme.Satisfied satisfied)) {
                continue;
            }
            List<List<Expression>> assignment = new ArrayList<>();
            for (int lambdaIndex : lambdaIndexes) {
                // The formal is materialized against the probing arguments; arity matches the lambda
                // because unification succeeded against its function type over fresh variables
                if (!(satisfied.result().parameterTypes().get(lambdaIndex) instanceof Expression.FunctionType formal)
                        || !formal.parameterTypes().stream().allMatch(Expression::isGround)) {
                    assignment = null;
                    break;
                }
                assignment.add(List.copyOf(formal.parameterTypes()));
            }
            if (assignment != null) {
                assignments.add(assignment);
            }
        }
        if (assignments.isEmpty()) {
            throw new TypeCheckFailure(TypeCheckFailure.Kind.NO_MATCH, "No candidate of '%s' determines the lambda parameter types".formatted(name));
        }
        return assignments;
    }

    /// Resolves the call and transcribes the resolution's argument coercions onto the argument
    /// nodes, the way the analyzer records them for the planner — skipping conversions that do
    /// not change the Trino type (e.g. unbounded varchar re-encoded as its length sentinel)
    private Type resolveRecordingCoercions(String name, List<io.trino.sql.tree.Expression> argumentNodes, List<FunctionResolver.Argument> arguments)
    {
        if (resolveOutcome(name, arguments) instanceof FunctionResolver.Ambiguous ambiguous) {
            // The engine treats candidates that agree on the return type as semantically the
            // same and picks one arbitrarily (FunctionBinder's unknown-argument tie-break).
            // The pick decides the formals, so the arguments' coercions are unverifiable
            Set<Expression> returnTypes = ambiguous.candidates().stream()
                    .map(FunctionResolver.Resolution::returnType)
                    .collect(toSet());
            if (returnTypes.size() == 1) {
                argumentNodes.forEach(node -> unverifiedCoercions.add(NodeRef.of(node)));
                return toType(returnTypes.iterator().next());
            }
        }
        FunctionResolver.Resolution resolution = resolve(name, arguments);
        for (FunctionResolver.ArgumentCoercion coercion : resolution.argumentCoercions()) {
            if (coercion.coercionNeeded()) {
                Type formal = toType(coercion.formalType());
                if (!formal.equals(toType(coercion.actualType()))) {
                    coercions.put(NodeRef.of(argumentNodes.get(coercion.index())), formal);
                }
            }
        }
        return toType(resolution.returnType());
    }

    private FunctionResolver.Resolution resolve(String name, List<FunctionResolver.Argument> arguments)
    {
        return switch (resolveOutcome(name, arguments)) {
            case FunctionResolver.Resolved resolved -> resolved.resolution();
            case FunctionResolver.Ambiguous ambiguous -> throw new TypeCheckFailure(TypeCheckFailure.Kind.AMBIGUOUS, "Ambiguous call to '%s': %s candidates".formatted(name, ambiguous.candidates().size()));
            // Incomplete means no candidate fully bound, but some could not be refuted either —
            // the solver keeps them as near-misses for diagnostics. With ground arguments an
            // applicable candidate always binds fully, so this is a rejection like NoMatch,
            // just with better error-reporting material
            case FunctionResolver.Incomplete incomplete -> throw new TypeCheckFailure(TypeCheckFailure.Kind.NO_MATCH, "No match for '%s' with arguments %s — near-miss candidates: %s".formatted(name, arguments, incomplete.candidates()));
            case FunctionResolver.NoMatch _ -> throw new TypeCheckFailure(TypeCheckFailure.Kind.NO_MATCH, "No match for '%s' with arguments %s".formatted(name, arguments));
        };
    }

    private Type toType(Expression expression)
    {
        return typeManager.getType(TypeBridge.toTypeDescriptor(expression));
    }
}
