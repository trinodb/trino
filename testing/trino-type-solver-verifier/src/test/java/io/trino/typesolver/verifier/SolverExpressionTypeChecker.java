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

import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
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
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Predicated;
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
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeScheme;
import org.weakref.solver.VariableAllocator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toTypeDescriptor;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.variable;

/// Stage-1 prototype of solver-based expression type checking: types a scalar expression tree
/// against a solver [TypeLibrary] instead of the bespoke `ExpressionAnalyzer`.
///
/// Each call is one constraint problem. A lambda argument enters the solve as a function type over
/// fresh variables — there is no eager/deferred argument split (no `TypeDescriptorProvider`): the
/// lambda's parameter types fall out of unifying the *other* arguments against the candidate's
/// scheme. The one irreducible analyzer obligation — typing the lambda *body* once its parameter
/// types are determined — is discharged between the probing solve and the final solve, which then
/// runs with all arguments ground so full overload dominance applies.
final class SolverExpressionTypeChecker
{
    private final TypeLibrary library;
    private final TypeManager typeManager;
    private final VariableAllocator allocator = new VariableAllocator();
    private final Map<NodeRef<io.trino.sql.tree.Expression>, Type> types = new HashMap<>();
    private final Map<NodeRef<io.trino.sql.tree.Expression>, Type> coercions = new HashMap<>();

    SolverExpressionTypeChecker(TypeLibrary library, TypeManager typeManager)
    {
        this.library = requireNonNull(library, "library is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        // Keep the checker's fresh variables visibly distinct from scheme-instantiation variables
        allocator.reserveThrough(1_000_000);
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
        Type type = typeOf(expression, Map.of());
        return new Analysis(type, Map.copyOf(types), Map.copyOf(coercions));
    }

    public record Analysis(
            Type type,
            Map<NodeRef<io.trino.sql.tree.Expression>, Type> types,
            Map<NodeRef<io.trino.sql.tree.Expression>, Type> coercions) {}

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
        Type type = uncachedTypeOf(node, scope);
        types.put(NodeRef.of(node), type);
        return type;
    }

    private Type uncachedTypeOf(io.trino.sql.tree.Expression node, Map<String, Type> scope)
    {
        return switch (node) {
            case LongLiteral literal -> literal.getParsedValue() == (int) literal.getParsedValue() ? INTEGER : BIGINT;
            case DecimalLiteral literal -> Decimals.parse(literal.getValue()).getType();
            case DoubleLiteral _ -> DOUBLE;
            case BooleanLiteral _ -> BOOLEAN;
            case StringLiteral literal -> createVarcharType(literal.getValue().length());
            case Identifier identifier -> requireNonNull(scope.get(identifier.getValue()), () -> "Unbound identifier: " + identifier.getValue());
            case Cast cast -> typeManager.getType(toTypeDescriptor(cast.getType()));
            case NullLiteral _ -> UNKNOWN;
            case ArithmeticBinaryExpression operation -> resolveCall(operation.getOperator().getValue(), List.of(operation.getLeft(), operation.getRight()), scope);
            case Predicated predicated -> {
                // The LHS rides on the Predicated wrapper; each fragment reproduces the
                // constraint shape its former standalone node carried.
                io.trino.sql.tree.Expression value = predicated.getValue();
                yield switch (predicated.getPredicate()) {
                    case ComparisonPredicate comparison -> resolveCall(comparison.getOperator().getValue(), List.of(value, comparison.getRight()), scope);
                    case IsNullPredicate _ -> {
                        // IS [NOT] NULL is a boolean of any argument; negation does not affect typing.
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
            case FunctionCall call -> resolveCall(call.getName().getSuffix(), call.getArguments().stream().map(CallArgument::getValue).toList(), scope);
            case SubscriptExpression subscript -> resolveCall("[]", List.of(subscript.getBase(), subscript.getIndex()), scope);
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
            case io.trino.sql.tree.Row row -> {
                Expression.RowField[] fields = row.getFields().stream()
                        .map(field -> {
                            Expression type = TypeBridge.toExpression(typeOf(field.getExpression(), scope));
                            return field.getName()
                                    .map(name -> Expression.field(name.getValue(), type))
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
        if (!type.equals(BOOLEAN)) {
            throw new TypeCheckFailure(TypeCheckFailure.Kind.BOOLEAN_REQUIRED, "%s must be boolean, found %s: %s".formatted(role, type, node));
        }
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
        if (!(scheme.matchFunctionCallOutcome(arguments, library.typeSystem()) instanceof TypeScheme.Satisfied satisfied)) {
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

    private Type resolveCall(String name, List<io.trino.sql.tree.Expression> argumentNodes, Map<String, Type> scope)
    {
        record PendingLambda(int index, LambdaExpression lambda, List<String> parameterVariables) {}

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
            return resolveRecordingCoercions(name, argumentNodes, arguments);
        }

        // Probing solve, per candidate: with the lambdas as function types over fresh variables,
        // unifying the other arguments against a candidate's scheme determines each lambda formal's
        // parameter types. Different candidates may assign different parameter types, under which a
        // lambda body may type differently — so the obligation is discharged per distinct assignment,
        // and each completed assignment gets a final solve over ground arguments with full dominance.
        Set<List<List<Expression>>> assignments = probe(name, arguments, pendingLambdas.stream().map(PendingLambda::index).toList());

        Set<Type> results = new LinkedHashSet<>();
        for (List<List<Expression>> assignment : assignments) {
            List<Expression> completed = new ArrayList<>(arguments);
            for (int lambdaOrdinal = 0; lambdaOrdinal < pendingLambdas.size(); lambdaOrdinal++) {
                PendingLambda pending = pendingLambdas.get(lambdaOrdinal);
                List<Expression> parameterTypes = assignment.get(lambdaOrdinal);
                Map<String, Type> lambdaScope = new HashMap<>(scope);
                for (int parameter = 0; parameter < parameterTypes.size(); parameter++) {
                    lambdaScope.put(pending.lambda().getArguments().get(parameter).getName().getValue(), toType(parameterTypes.get(parameter)));
                }
                // The analyzer obligation: type the lambda body with its parameter types in scope
                Type bodyType = typeOf(pending.lambda().getBody(), lambdaScope);
                completed.set(pending.index(), function(parameterTypes, TypeBridge.toExpression(bodyType)));
            }
            // Final solve with every argument ground: full overload dominance applies. With
            // several surviving assignments the last one's coercions win; the ambiguity check
            // below rejects the call unless they agree on the result anyway
            results.add(resolveRecordingCoercions(name, argumentNodes, completed));
        }

        if (results.size() != 1) {
            throw new TypeCheckFailure(TypeCheckFailure.Kind.AMBIGUOUS, "Call to '%s' is ambiguous: lambda arguments type differently under different candidates: %s".formatted(name, results));
        }
        return results.iterator().next();
    }

    /// Probes every candidate and returns the distinct ground parameter-type assignments for the
    /// lambda arguments: one entry per assignment, holding the parameter types of each lambda in
    /// order. Candidates that do not determine all lambda parameter types are skipped.
    private Set<List<List<Expression>>> probe(String name, List<Expression> arguments, List<Integer> lambdaIndexes)
    {
        Set<List<List<Expression>>> assignments = new LinkedHashSet<>();
        for (TypeScheme candidate : library.functions(name)) {
            if (!(candidate.matchFunctionCallOutcome(arguments, library.typeSystem()) instanceof TypeScheme.Satisfied satisfied)) {
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
    private Type resolveRecordingCoercions(String name, List<io.trino.sql.tree.Expression> argumentNodes, List<Expression> arguments)
    {
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

    private FunctionResolver.Resolution resolve(String name, List<Expression> arguments)
    {
        return switch (library.resolveFunction(name, arguments)) {
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
        return typeManager.getType(TypeId.of(TypeBridge.render(expression)));
    }
}
