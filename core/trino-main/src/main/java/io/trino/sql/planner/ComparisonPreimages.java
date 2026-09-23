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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.metadata.FunctionPreimages;
import io.trino.metadata.FunctionPreimages.BoundPreimage;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.function.PreimageResult.Exactness;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.bindIfNecessary;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.type.BooleanOperators.NOT_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

/// Shared predicate lowering and bound projection lookup for exact rewrites and domain extraction.
public final class ComparisonPreimages
{
    private static final int MAX_IN_LIST_SIZE = 10;
    private static final int MAX_RANGES = 32;
    private static final Constant UNKNOWN = new Constant(BOOLEAN, null);

    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final FunctionPreimages functionPreimages;
    private final Session session;
    private final ValueSetToExpression renderer;

    public ComparisonPreimages(PlannerContext plannerContext, Session session)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.functionPreimages = new FunctionPreimages(metadata, plannerContext.getFunctionManager(), plannerContext.getTypeManager(), session);
        this.session = requireNonNull(session, "session is null");
        this.renderer = new ValueSetToExpression(metadata);
    }

    public Optional<Expression> rewrite(Expression expression, SymbolAllocator allocator)
    {
        if (expression instanceof In in && in.valueList().size() > MAX_IN_LIST_SIZE) {
            return rewriteLargeIn(in).or(() -> rewriteComparisonConstants(expression));
        }
        var comparison = matchComparison(expression);
        if (comparison != null) {
            Expression left = comparisonIdentity(comparison.left());
            Expression right = comparisonIdentity(comparison.right());
            if (!left.equals(comparison.left()) || !right.equals(comparison.right())) {
                return Optional.of(comparison(metadata, getCharVarcharCoercion(session), comparison.operator(), left, right));
            }
        }
        Optional<Predicate> predicate = analyze(expression);
        if (predicate.isEmpty()) {
            return rewriteComparisonConstants(expression);
        }
        Optional<BoundProjection> bound = bind(predicate.get().operand(), EXACT);
        if (bound.isEmpty()) {
            return Optional.empty();
        }
        Optional<PreimageResult> trueResult = bound.get().project(predicate.get().domains().trueDomain());
        Optional<PreimageResult> falseResult = bound.get().project(predicate.get().domains().falseDomain());
        if (trueResult.isEmpty() || falseResult.isEmpty()) {
            return Optional.empty();
        }
        // Exact projections may overlap on inputs where the original function fails.
        if (trueResult.get().inputDomain().overlaps(falseResult.get().inputDomain())) {
            return Optional.empty();
        }
        TruthDomains inputDomains = new TruthDomains(trueResult.get().inputDomain(), falseResult.get().inputDomain());
        if (!bounded(inputDomains)) {
            return Optional.empty();
        }
        Expression input = bound.get().input();
        Expression rendered = render(inputDomains, input);
        if (occurrences(rendered, input) <= 1) {
            return Optional.of(rendered);
        }
        return Optional.of(bindIfNecessary(allocator, "operand", input, operand -> render(inputDomains, operand)));
    }

    private Optional<Expression> rewriteComparisonConstants(Expression expression)
    {
        var comparison = matchComparison(expression);
        if (comparison == null && !(expression instanceof In)) {
            return Optional.empty();
        }
        Optional<Expression> operand = findOperand(expression);
        if (operand.isEmpty()) {
            return Optional.empty();
        }
        Optional<BoundProjection> bound = bind(operand.get(), EXACT, false);
        if (bound.isEmpty() || (hasSupportedComparisons(operand.get().type()) && hasSupportedComparisons(bound.get().input().type()))) {
            return Optional.empty();
        }
        Function<Constant, Optional<Constant>> map = constant -> bound.get().preimage()
                .comparisonConstant(new NullableValue(constant.type(), constant.value()))
                .map(value -> new Constant(value.getType(), value.getValue()));
        if (comparison != null) {
            boolean reversed = comparison.left() instanceof Constant;
            Constant constant = (Constant) (reversed ? comparison.left() : comparison.right());
            return map.apply(constant).map(value -> comparison(
                    metadata,
                    getCharVarcharCoercion(session),
                    reversed ? comparison.operator().flip() : comparison.operator(),
                    bound.get().input(),
                    value));
        }
        if (expression instanceof In in) {
            List<Expression> values = new ArrayList<>();
            for (Expression item : in.valueList()) {
                if (!(item instanceof Constant constant)) {
                    return Optional.empty();
                }
                Optional<Constant> value = map.apply(constant);
                if (value.isEmpty()) {
                    return Optional.empty();
                }
                values.add(value.get());
            }
            return Optional.of(new In(bound.get().input(), values));
        }
        return Optional.empty();
    }

    /// A point-to-point IN rewrite keeps its original size and needs no expansion budget.
    private Optional<Expression> rewriteLargeIn(In in)
    {
        Optional<BoundProjection> bound = bind(in.value(), EXACT);
        if (bound.isEmpty()) {
            return Optional.empty();
        }
        List<Expression> values = new ArrayList<>();
        Type inputType = bound.get().input().type();
        for (Expression item : in.valueList()) {
            if (!(item instanceof Constant constant)) {
                return Optional.empty();
            }
            if (constant.value() == null) {
                values.add(new Constant(inputType, null));
                continue;
            }
            TruthDomains result = TruthDomains.comparison(EQUAL, new NullableValue(constant.type(), constant.value()));
            Optional<PreimageResult> truth = bound.get().project(result.trueDomain());
            Optional<PreimageResult> falsity = bound.get().project(result.falseDomain());
            if (truth.isEmpty() || falsity.isEmpty() || !truth.get().inputDomain().union(falsity.get().inputDomain()).equals(Domain.notNull(inputType))) {
                return Optional.empty();
            }
            // SQL equality cannot express membership in a NaN singleton.
            if (includesNaN(truth.get().inputDomain())) {
                return Optional.empty();
            }
            Optional<Constant> point = singleton(truth.get().inputDomain());
            if (point.isEmpty()) {
                return Optional.empty();
            }
            values.add(point.get());
        }
        return Optional.of(new In(bound.get().input(), values));
    }

    private static int occurrences(Expression expression, Expression input)
    {
        if (expression.equals(input)) {
            return 1;
        }
        return expression.children().stream().mapToInt(child -> occurrences(child, input)).sum();
    }

    public Optional<ProjectedDomain> extract(Expression expression, boolean complement)
    {
        Optional<Predicate> predicate = analyze(expression);
        if (predicate.isEmpty()) {
            return Optional.empty();
        }
        Domain domain = complement ? predicate.get().domains().falseDomain() : predicate.get().domains().trueDomain();
        Expression operand = predicate.get().operand();
        Exactness exactness = EXACT;
        for (int depth = 0; depth < MAX_RANGES; depth++) {
            Optional<BoundProjection> bound = bind(operand, CONSERVATIVE);
            if (bound.isEmpty()) {
                return Optional.empty();
            }
            Optional<PreimageResult> result = bound.get().project(domain);
            if (result.isEmpty() || !bounded(result.get().inputDomain())) {
                return Optional.empty();
            }
            domain = result.get().inputDomain();
            if (result.get().exactness() == CONSERVATIVE) {
                exactness = CONSERVATIVE;
            }
            operand = bound.get().input();
            if (operand instanceof Reference reference) {
                return Optional.of(new ProjectedDomain(reference, domain, exactness));
            }
        }
        return Optional.empty();
    }

    public boolean hasProjection(Expression expression)
    {
        return bind(expression, CONSERVATIVE).isPresent();
    }

    private Optional<BoundProjection> bind(Expression expression, Exactness exactness)
    {
        return bind(expression, exactness, true);
    }

    private Optional<BoundProjection> bind(Expression expression, Exactness exactness, boolean requireTruthDomains)
    {
        ResolvedFunction function;
        List<Expression> arguments;
        if (expression instanceof Call call) {
            function = call.function();
            arguments = call.arguments();
        }
        else if (expression instanceof Cast cast) {
            function = metadata.getCoercion(getCharVarcharCoercion(session), cast.expression().type(), cast.type());
            arguments = List.of(cast.expression());
        }
        else {
            return Optional.empty();
        }
        if (!function.deterministic()) {
            return Optional.empty();
        }
        int projectedArgument = -1;
        List<Optional<NullableValue>> constants = new ArrayList<>();
        for (int index = 0; index < arguments.size(); index++) {
            if (arguments.get(index) instanceof Constant constant) {
                if (constant.value() == null) {
                    return Optional.empty();
                }
                constants.add(Optional.of(new NullableValue(constant.type(), constant.value())));
            }
            else {
                if (projectedArgument != -1) {
                    return Optional.empty();
                }
                projectedArgument = index;
                constants.add(Optional.empty());
            }
        }
        if (projectedArgument == -1 || (requireTruthDomains && (!hasSupportedComparisons(function.signature().getReturnType()) || !hasSupportedComparisons(arguments.get(projectedArgument).type())))) {
            return Optional.empty();
        }
        Expression input = arguments.get(projectedArgument);
        return functionPreimages.bind(function, projectedArgument, constants, exactness).map(bound -> new BoundProjection(input, bound));
    }

    private Expression comparisonIdentity(Expression expression)
    {
        for (int depth = 0; depth < MAX_RANGES; depth++) {
            Optional<BoundProjection> projection = bind(expression, EXACT);
            if (projection.isEmpty() || !projection.get().preimage().isComparisonIdentity()) {
                break;
            }
            expression = projection.get().input();
        }
        return expression;
    }

    private Optional<Predicate> analyze(Expression expression)
    {
        if (expression instanceof Let let) {
            if (!hasProjection(let.value())) {
                return Optional.empty();
            }
            return truthDomains(let.body(), let.name().toSymbolReference()).map(domains -> new Predicate(let.value(), domains));
        }
        Optional<Expression> operand = findOperand(expression).filter(this::hasProjection);
        return operand.flatMap(value -> truthDomains(expression, value).map(domains -> new Predicate(value, domains)));
    }

    private Optional<Expression> findOperand(Expression expression)
    {
        var comparison = matchComparison(expression);
        if (comparison != null) {
            if (comparison.right() instanceof Constant) {
                return Optional.of(comparison.left());
            }
            if (comparison.left() instanceof Constant) {
                return Optional.of(comparison.right());
            }
        }
        if (expression instanceof In in) {
            return Optional.of(in.value());
        }
        if (isNot(expression)) {
            return findOperand(((Call) expression).arguments().getFirst());
        }
        if (expression instanceof Logical logical) {
            return findOperand(logical.terms().getFirst());
        }
        return Optional.empty();
    }

    private boolean hasSupportedComparisons(Type type)
    {
        if (!type.isOrderable()) {
            return false;
        }
        // Generic equality metadata is nullable even for primitive types. Check the type's
        // actual operator declarations before treating non-null values as two-valued.
        var operators = type.getTypeOperatorDeclaration(plannerContext.getTypeOperators());
        return operators != null && !operators.getEqualOperators().isEmpty() && List.of(
                        operators.getEqualOperators(),
                        operators.getComparisonUnorderedLastOperators(),
                        operators.getComparisonUnorderedFirstOperators(),
                        operators.getLessThanOperators(),
                        operators.getLessThanOrEqualOperators()).stream()
                .flatMap(Collection::stream)
                .allMatch(operator -> operator.getCallingConvention().getReturnConvention() == FAIL_ON_NULL);
    }

    private Optional<TruthDomains> truthDomains(Expression expression, Expression operand)
    {
        if (expression instanceof IsNull isNull && isNull.value().equals(operand)) {
            return Optional.of(new TruthDomains(Domain.onlyNull(operand.type()), Domain.notNull(operand.type())));
        }
        var comparison = matchComparison(expression);
        if (comparison != null) {
            if (comparison.left().equals(operand) && comparison.right() instanceof Constant constant) {
                return Optional.of(TruthDomains.comparison(comparison.operator(), new NullableValue(constant.type(), constant.value())));
            }
            if (comparison.right().equals(operand) && comparison.left() instanceof Constant constant) {
                return Optional.of(TruthDomains.comparison(comparison.operator().flip(), new NullableValue(constant.type(), constant.value())));
            }
            return Optional.empty();
        }
        if (expression instanceof In in && in.value().equals(operand)) {
            if (in.valueList().isEmpty() || in.valueList().size() > MAX_IN_LIST_SIZE) {
                return Optional.empty();
            }
            TruthDomains domains = new TruthDomains(Domain.none(operand.type()), Domain.all(operand.type()));
            for (Expression item : in.valueList()) {
                if (!(item instanceof Constant constant)) {
                    return Optional.empty();
                }
                domains = domains.or(TruthDomains.comparison(EQUAL, new NullableValue(constant.type(), constant.value())));
                if (!bounded(domains)) {
                    return Optional.empty();
                }
            }
            return Optional.of(domains);
        }
        if (isNot(expression)) {
            return truthDomains(((Call) expression).arguments().getFirst(), operand).map(TruthDomains::not);
        }
        if (expression instanceof Logical logical) {
            if (logical.terms().size() > MAX_RANGES || !isDeterministic(operand) || mayFail(plannerContext, getCharVarcharCoercion(session), operand)) {
                return Optional.empty();
            }
            TruthDomains domains = logical.operator() == AND
                    ? new TruthDomains(Domain.all(operand.type()), Domain.none(operand.type()))
                    : new TruthDomains(Domain.none(operand.type()), Domain.all(operand.type()));
            for (Expression term : logical.terms()) {
                Optional<TruthDomains> next = truthDomains(term, operand);
                if (next.isEmpty()) {
                    return Optional.empty();
                }
                domains = logical.operator() == AND ? domains.and(next.get()) : domains.or(next.get());
                if (!bounded(domains)) {
                    return Optional.empty();
                }
            }
            return Optional.of(domains);
        }
        if (expression instanceof Constant constant && constant.type().equals(BOOLEAN)) {
            return Optional.of(new TruthDomains(
                    Boolean.TRUE.equals(constant.value()) ? Domain.all(operand.type()) : Domain.none(operand.type()),
                    Boolean.FALSE.equals(constant.value()) ? Domain.all(operand.type()) : Domain.none(operand.type())));
        }
        return Optional.empty();
    }

    private Expression render(TruthDomains domains, Expression operand)
    {
        Domain trueDomain = domains.trueDomain();
        Domain falseDomain = domains.falseDomain();
        Domain known = trueDomain.union(falseDomain);
        if (known.equals(Domain.notNull(operand.type()))) {
            Expression predicate = nonNullMembership(trueDomain, operand);
            if (predicate instanceof Constant) {
                return predicate.equals(TRUE) ? or(not(metadata, getCharVarcharCoercion(session), new IsNull(operand)), UNKNOWN) : and(new IsNull(operand), UNKNOWN);
            }
            if (trueDomain.getValues() instanceof FloatingPointValueSet floatingPoint && floatingPoint.isNaNAllowed() && floatingPoint.getOrderedValues().isNone()) {
                return ifExpression(new IsNull(operand), UNKNOWN, predicate);
            }
            return predicate;
        }
        if (known.isAll()) {
            return membership(trueDomain, operand);
        }
        if (trueDomain.isNone() && falseDomain.isNone()) {
            return UNKNOWN;
        }
        if (falseDomain.isNone() && !trueDomain.isNullAllowed() && !includesNaN(trueDomain) && trueDomain.getValues().isDiscreteSet()) {
            List<Expression> values = new ArrayList<>();
            for (Object value : trueDomain.getValues().getDiscreteSet()) {
                values.add(new Constant(operand.type(), value));
            }
            values.add(new Constant(operand.type(), null));
            return new In(operand, values);
        }
        if (falseDomain.isNone()) {
            return ifExpression(membership(trueDomain, operand), TRUE, UNKNOWN);
        }
        if (trueDomain.isNone()) {
            return ifExpression(membership(falseDomain, operand), FALSE, UNKNOWN);
        }
        return new Case(List.of(new WhenClause(membership(trueDomain, operand), TRUE), new WhenClause(membership(falseDomain, operand), FALSE)), UNKNOWN);
    }

    private Expression nonNullMembership(Domain domain, Expression operand)
    {
        return renderer.toExpression(getCharVarcharCoercion(session), ValueSetToExpression.simplifyValues(domain.getValues()), operand);
    }

    private static boolean includesNaN(Domain domain)
    {
        return typeHasNaN(domain.getType()) && domain.includesNullableValue(FloatingPointValueSet.nanValue(domain.getType()));
    }

    private Expression membership(Domain domain, Expression operand)
    {
        Optional<Constant> point = singleton(domain);
        if (point.isPresent()) {
            return comparison(metadata, getCharVarcharCoercion(session), IDENTICAL, operand, point.get());
        }
        Optional<Constant> excluded = singleton(domain.complement());
        if (excluded.isPresent()) {
            return not(metadata, getCharVarcharCoercion(session), comparison(metadata, getCharVarcharCoercion(session), IDENTICAL, operand, excluded.get()));
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? new IsNull(operand) : FALSE;
        }
        if (domain.equals(Domain.all(operand.type())) || domain.equals(Domain.notNull(operand.type()))) {
            return domain.isNullAllowed() ? TRUE : not(metadata, getCharVarcharCoercion(session), new IsNull(operand));
        }
        Expression predicate = nonNullMembership(domain, operand);
        return domain.isNullAllowed()
                ? or(new IsNull(operand), predicate)
                : new Logical(AND, List.of(not(metadata, getCharVarcharCoercion(session), new IsNull(operand)), predicate));
    }

    private static Optional<Constant> singleton(Domain domain)
    {
        if (domain.isNullAllowed()) {
            return Optional.empty();
        }
        if (domain.getValues().isSingleValue()) {
            return Optional.of(new Constant(domain.getType(), domain.getValues().getSingleValue()));
        }
        if (!includesNaN(domain) && domain.getType().getRange().isPresent() && domain.getValues().getRanges().getRangeCount() == 1) {
            var range = domain.getValues().getRanges().getOrderedRanges().getFirst();
            var bounds = domain.getType().getRange().orElseThrow();
            if (!range.isLowUnbounded() && range.isLowInclusive() && range.getLowBoundedValue().equals(bounds.getMax())) {
                return Optional.of(new Constant(domain.getType(), bounds.getMax()));
            }
            if (!range.isHighUnbounded() && range.isHighInclusive() && range.getHighBoundedValue().equals(bounds.getMin())) {
                return Optional.of(new Constant(domain.getType(), bounds.getMin()));
            }
        }
        return Optional.empty();
    }

    private static boolean isNot(Expression expression)
    {
        return expression instanceof Call call && call.function().name().equals(builtinFunctionName(NOT_FUNCTION_NAME)) && call.arguments().size() == 1;
    }

    private static boolean bounded(TruthDomains domains)
    {
        return rangeCount(domains.trueDomain()) + rangeCount(domains.falseDomain()) <= MAX_RANGES;
    }

    private static boolean bounded(Domain domain)
    {
        return rangeCount(domain) <= MAX_RANGES;
    }

    private static int rangeCount(Domain domain)
    {
        if (domain.getValues() instanceof FloatingPointValueSet floatingPoint) {
            return floatingPoint.getOrderedValues().getRangeCount() + (floatingPoint.isNaNAllowed() ? 1 : 0);
        }
        return domain.getValues().getRanges().getRangeCount();
    }

    private record Predicate(Expression operand, TruthDomains domains) {}

    private record BoundProjection(Expression input, BoundPreimage preimage)
    {
        private Optional<PreimageResult> project(Domain resultDomain)
        {
            return preimage.project(resultDomain);
        }
    }

    public record ProjectedDomain(Reference reference, Domain domain, Exactness exactness) {}
}
