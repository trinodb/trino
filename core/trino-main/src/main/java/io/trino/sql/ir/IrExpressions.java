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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.type.TypeCoercion;
import jakarta.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULO;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.isDynamicFilterFunction;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.type.BooleanOperators.NOT_FUNCTION_NAME;

public final class IrExpressions
{
    private IrExpressions() {}

    public static Constant constantNull(Type type)
    {
        return new Constant(type, null);
    }

    public static Call call(ResolvedFunction function, Expression... arguments)
    {
        return new Call(function, Arrays.asList(arguments));
    }

    public static Expression comparison(Metadata metadata, ComparisonOperator operator, Expression left, Expression right)
    {
        return switch (operator) {
            case EQUAL -> operatorCall(metadata, OperatorType.EQUAL, left, right);
            case NOT_EQUAL -> not(metadata, operatorCall(metadata, OperatorType.EQUAL, left, right));
            case LESS_THAN -> operatorCall(metadata, OperatorType.LESS_THAN, left, right);
            case LESS_THAN_OR_EQUAL -> operatorCall(metadata, OperatorType.LESS_THAN_OR_EQUAL, left, right);
            case GREATER_THAN -> operatorCall(metadata, OperatorType.LESS_THAN, right, left);
            case GREATER_THAN_OR_EQUAL -> operatorCall(metadata, OperatorType.LESS_THAN_OR_EQUAL, right, left);
            case IDENTICAL -> operatorCall(metadata, OperatorType.IDENTICAL, left, right);
        };
    }

    private static Call operatorCall(Metadata metadata, OperatorType operator, Expression left, Expression right)
    {
        return call(metadata.resolveOperator(operator, ImmutableList.of(left.type(), right.type())), left, right);
    }

    /// Decodes the canonical IR form of a comparison back into its operator and operands, or
    /// returns null when the expression is not a comparison. Recognizes the operator-function calls
    /// produced by {@link #comparison} as well as the `$not($operator$EQUAL(...))` form of `<>`.
    /// Because greater-than comparisons are canonicalized to `LessThan` / `LessThanOrEqual` with
    /// flipped operands, this never yields `GreaterThan` or `GreaterThanOrEqual`.
    @Nullable
    public static Comparison matchComparison(Expression expression)
    {
        if (!(expression instanceof Call call)) {
            return null;
        }

        List<Expression> arguments = call.arguments();
        if (call.function().name().equals(builtinFunctionName(NOT_FUNCTION_NAME)) && arguments.size() == 1) {
            if (matchComparison(arguments.get(0)) instanceof Comparison.Equal(Expression left, Expression right)) {
                return new Comparison.NotEqual(left, right);
            }
            return null;
        }

        Optional<OperatorType> operatorType = operatorType(call);
        if (operatorType.isEmpty()) {
            return null;
        }
        return switch (operatorType.get()) {
            case EQUAL -> new Comparison.Equal(arguments.get(0), arguments.get(1));
            case LESS_THAN -> new Comparison.LessThan(arguments.get(0), arguments.get(1));
            case LESS_THAN_OR_EQUAL -> new Comparison.LessThanOrEqual(arguments.get(0), arguments.get(1));
            case IDENTICAL -> new Comparison.Identical(arguments.get(0), arguments.get(1));
            default -> null;
        };
    }

    private static Optional<OperatorType> operatorType(Call call)
    {
        CatalogSchemaFunctionName name = call.function().name();
        if (isBuiltinFunctionName(name) && isOperatorName(name.functionName())) {
            return Optional.of(unmangleOperator(name.functionName()));
        }
        return Optional.empty();
    }

    /// A comparison recovered by {@link #matchComparison}, modeled as one of the five canonical
    /// operators so callers can pattern-match on it — for example
    /// `matchComparison(e) instanceof Comparison.LessThan(Expression l, Expression r)` — with the
    /// compiler enforcing exhaustiveness. Greater-than forms never appear: they are canonicalized to
    /// flipped `LessThan` / `LessThanOrEqual`.
    public sealed interface Comparison
            permits Comparison.Equal,
                    Comparison.Identical,
                    Comparison.LessThan,
                    Comparison.LessThanOrEqual,
                    Comparison.NotEqual
    {
        ComparisonOperator operator();

        Expression left();

        Expression right();

        record Equal(Expression left, Expression right)
                implements Comparison
        {
            @Override
            public ComparisonOperator operator()
            {
                return ComparisonOperator.EQUAL;
            }
        }

        record NotEqual(Expression left, Expression right)
                implements Comparison
        {
            @Override
            public ComparisonOperator operator()
            {
                return ComparisonOperator.NOT_EQUAL;
            }
        }

        record LessThan(Expression left, Expression right)
                implements Comparison
        {
            @Override
            public ComparisonOperator operator()
            {
                return ComparisonOperator.LESS_THAN;
            }
        }

        record LessThanOrEqual(Expression left, Expression right)
                implements Comparison
        {
            @Override
            public ComparisonOperator operator()
            {
                return ComparisonOperator.LESS_THAN_OR_EQUAL;
            }
        }

        record Identical(Expression left, Expression right)
                implements Comparison
        {
            @Override
            public ComparisonOperator operator()
            {
                return ComparisonOperator.IDENTICAL;
            }
        }
    }

    /// Lower a BETWEEN to `value >= min AND value <= max`, wrapping `value` in a [Let] when it is
    /// non-trivial so the operand is evaluated exactly once.
    public static Expression between(Metadata metadata, SymbolAllocator allocator, Expression value, Expression min, Expression max)
    {
        // Inline trivial values directly so the result stays a plain AND of comparisons.
        if (value instanceof Reference || value instanceof Constant) {
            return new Logical(AND, ImmutableList.of(
                    comparison(metadata, GREATER_THAN_OR_EQUAL, value, min),
                    comparison(metadata, LESS_THAN_OR_EQUAL, value, max)));
        }
        Symbol bound = allocator.newSymbol("between", value.type());
        Reference reference = new Reference(value.type(), bound.name());
        return new Let(bound, value, new Logical(AND, ImmutableList.of(
                comparison(metadata, GREATER_THAN_OR_EQUAL, reference, min),
                comparison(metadata, LESS_THAN_OR_EQUAL, reference, max))));
    }

    /// Recognize a BETWEEN-shape as produced by [#between]. `between` builds `min <= value AND
    /// value <= max` (greater-than forms are canonicalized to flipped `LessThanOrEqual`), in either
    /// the trivial-value form or a `Let`-wrapped form binding `value` to a fresh symbol.
    @Nullable
    public static Between matchBetween(Expression expression)
    {
        if (expression instanceof Let(Symbol bound, Expression value, Expression body)) {
            // The bound symbol must not leak through min/max: a pattern consumer sees only the
            // extracted parts, where such a reference would be unbound.
            if (matchAndRange(body) instanceof Between bounds
                    && bounds.value() instanceof Reference ref && ref.name().equals(bound.name())
                    && !SymbolsExtractor.extractUnique(bounds.min()).contains(bound)
                    && !SymbolsExtractor.extractUnique(bounds.max()).contains(bound)) {
                return new Between(value, bounds.min(), bounds.max());
            }
            return null;
        }
        return matchAndRange(expression);
    }

    @Nullable
    private static Between matchAndRange(Expression expression)
    {
        if (!(expression instanceof Logical logical) || logical.operator() != AND || logical.terms().size() != 2) {
            return null;
        }
        Comparison first = matchComparison(logical.terms().get(0));
        Comparison second = matchComparison(logical.terms().get(1));
        if (first == null || second == null) {
            return null;
        }
        // Canonical shape is `min <= value AND value <= max`: both conjuncts are LessThanOrEqual
        // sharing the `value` operand. Collapsing the two independent evaluations of `value` into
        // the pattern's single occurrence is only sound when `value` is deterministic.
        if (first.operator() == LESS_THAN_OR_EQUAL && second.operator() == LESS_THAN_OR_EQUAL
                && first.right().equals(second.left()) && isDeterministic(first.right())) {
            return new Between(first.right(), first.left(), second.right());
        }
        return null;
    }

    /// View of a `value BETWEEN min AND max` predicate as produced by [#between].
    public record Between(Expression value, Expression min, Expression max) {}

    /// Lower a NULLIF to `if(first = second) then null else first`, wrapping `first` in a [Let]
    /// when it is non-trivial so the operand is evaluated exactly once. Defaults the comparison
    /// type to `first.type()` — see the overload below for the mixed-type case.
    public static Expression nullIf(Metadata metadata, SymbolAllocator allocator, Expression first, Expression second)
    {
        return nullIf(metadata, allocator, first, second, first.type());
    }

    /// Same as [#nullIf(Metadata,SymbolAllocator,Expression,Expression)] but performs the equality at
    /// `comparisonType`, casting `first` and `second` as needed. The returned value keeps
    /// `first`'s type, matching SQL `NULLIF` semantics; the cast is applied only for the
    /// comparison.
    public static Expression nullIf(Metadata metadata, SymbolAllocator allocator, Expression first, Expression second, Type comparisonType)
    {
        Expression secondForComparison = second.type().equals(comparisonType) ? second : new Cast(second, comparisonType);
        // Inline trivial values directly so the result stays a plain Case expression.
        if (first instanceof Reference || first instanceof Constant) {
            Expression firstForComparison = first.type().equals(comparisonType) ? first : new Cast(first, comparisonType);
            return ifExpression(
                    comparison(metadata, EQUAL, firstForComparison, secondForComparison),
                    constantNull(first.type()),
                    first);
        }
        Symbol bound = allocator.newSymbol("nullif", first.type());
        Reference reference = new Reference(first.type(), bound.name());
        Expression referenceForComparison = first.type().equals(comparisonType) ? reference : new Cast(reference, comparisonType);
        return new Let(bound, first, ifExpression(
                comparison(metadata, EQUAL, referenceForComparison, secondForComparison),
                constantNull(first.type()),
                reference));
    }

    /// Recognize a NULLIF-shape as produced by [#nullIf] in either the trivial-value form
    /// (`if(first = second) then null else first`) or the Let-wrapped form
    /// (`Let(s, first, if(s = second) then null else s)`).
    @Nullable
    public static NullIf matchNullIf(Expression expression)
    {
        if (expression instanceof Let(Symbol bound, Expression first, Expression body)) {
            // The bound symbol must not leak through second: a pattern consumer sees only the
            // extracted parts, where such a reference would be unbound.
            if (matchIfFirstEqualsSecond(body) instanceof NullIf match
                    && match.first() instanceof Reference ref && ref.name().equals(bound.name())
                    && !SymbolsExtractor.extractUnique(match.second()).contains(bound)) {
                return new NullIf(first, match.second());
            }
            return null;
        }
        return matchIfFirstEqualsSecond(expression);
    }

    @Nullable
    private static NullIf matchIfFirstEqualsSecond(Expression expression)
    {
        if (!(expression instanceof Case caseExpression) || caseExpression.whenClauses().size() != 1) {
            return null;
        }
        WhenClause when = caseExpression.whenClauses().get(0);
        if (!(matchComparison(when.getOperand()) instanceof Comparison.Equal(Expression left, Expression right))) {
            return null;
        }
        if (!isConstantNull(when.getResult())) {
            return null;
        }
        if (!caseExpression.defaultValue().equals(left)) {
            return null;
        }
        // The first operand occurs in both the condition and the default: collapsing the two
        // independent evaluations into the pattern's single occurrence is only sound when it is
        // deterministic.
        if (!isDeterministic(left)) {
            return null;
        }
        return new NullIf(left, right);
    }

    /// View of a `NULLIF(first, second)` predicate as produced by [#nullIf].
    public record NullIf(Expression first, Expression second) {}

    public static Expression ifExpression(Expression condition, Expression trueCase)
    {
        return new Case(ImmutableList.of(new WhenClause(condition, trueCase)), constantNull(trueCase.type()));
    }

    public static Expression ifExpression(Expression condition, Expression trueCase, Expression falseCase)
    {
        return new Case(ImmutableList.of(new WhenClause(condition, trueCase)), falseCase);
    }

    /// Build a [MatchClause] that matches when the [Match] operand equals `value`.
    /// The clause's lambda is `(parameter) -> parameter = value`, where `parameter` is
    /// the operand-bound lambda parameter. The caller supplies `parameter` — it must be
    /// allocated through [io.trino.sql.planner.SymbolAllocator] so it cannot collide with a
    /// symbol referenced in `value` or one allocated later.
    public static MatchClause equalityClause(Metadata metadata, Symbol parameter, Expression value, Expression result)
    {
        return new MatchClause(
                new Lambda(
                        ImmutableList.of(parameter),
                        comparison(metadata, EQUAL, new Reference(parameter.type(), parameter.name()), value)),
                result);
    }

    public static Constant row(List<Constant> fields)
    {
        RowType type = RowType.anonymous(fields.stream()
                .map(Constant::type)
                .toList());

        return new Constant(
                type,
                buildRowValue(type, builders -> {
                    for (int i = 0; i < fields.size(); ++i) {
                        writeNativeValue(fields.get(i).type(), builders.get(i), fields.get(i).value());
                    }
                }));
    }

    public static boolean isConstantNull(Expression expression)
    {
        return expression instanceof Constant constant && constant.value() == null;
    }

    public static boolean mayBeNull(PlannerContext plannerContext, Expression expression)
    {
        return mayBeNull(plannerContext, expression, true);
    }

    /**
     * Returns true if the expression may return null when all symbol inputs are non-null.
     */
    public static boolean mayReturnNullOnNonNullInput(PlannerContext plannerContext, Expression expression)
    {
        return mayBeNull(plannerContext, expression, false);
    }

    private static boolean mayBeNull(PlannerContext plannerContext, Expression expression, boolean referencesMayBeNull)
    {
        return switch (expression) {
            // These expressions never return null
            case Array _, Bind _, IsNull _, Lambda _, Row _ -> false;

            // These expressions may return null based on their operands
            case Call e -> switch (matchComparison(e)) {
                case null -> mayBeNull(plannerContext, e.function(), e.arguments(), referencesMayBeNull);
                // IDENTICAL is null-safe; other comparisons return null only when one of their operands is null.
                case Comparison.Identical _ -> false;
                case Comparison comparison -> mayBeNull(plannerContext, comparison.left(), referencesMayBeNull) ||
                        mayBeNull(plannerContext, comparison.right(), referencesMayBeNull);
            };
            case Case e -> e.whenClauses().stream().anyMatch(clause -> mayBeNull(plannerContext, clause.getResult(), referencesMayBeNull)) ||
                    mayBeNull(plannerContext, e.defaultValue(), referencesMayBeNull);
            case Cast e -> mayBeNull(plannerContext, e, referencesMayBeNull);
            case Coalesce e -> e.operands().stream().allMatch(operand -> mayBeNull(plannerContext, operand, referencesMayBeNull));
            case In e -> mayBeNull(plannerContext, e.value(), referencesMayBeNull) || e.valueList().stream().anyMatch(value -> mayBeNull(plannerContext, value, referencesMayBeNull));
            case Let e -> mayBeNull(plannerContext, e.body(), referencesMayBeNull || mayBeNull(plannerContext, e.value(), referencesMayBeNull));
            case Logical e -> e.terms().stream().anyMatch(term -> mayBeNull(plannerContext, term, referencesMayBeNull));
            case Match e -> e.clauses().stream().anyMatch(clause -> mayBeNull(plannerContext, clause.result(), referencesMayBeNull)) ||
                    mayBeNull(plannerContext, e.defaultValue(), referencesMayBeNull);

            // These expressions may return null based on their own semantics
            case Constant e -> e.value() == null;
            case FieldReference _ -> true;
            case Reference _ -> referencesMayBeNull;
        };
    }

    private static boolean mayBeNull(PlannerContext plannerContext, Cast cast, boolean referencesMayBeNull)
    {
        if (cast.expression().type().equals(cast.type())) {
            return mayBeNull(plannerContext, cast.expression(), referencesMayBeNull);
        }

        ResolvedFunction coercion = plannerContext.getMetadata().getCoercion(cast.expression().type(), cast.type());
        return mayBeNull(plannerContext, coercion, ImmutableList.of(cast.expression()), referencesMayBeNull);
    }

    private static boolean mayBeNull(PlannerContext plannerContext, ResolvedFunction function, List<Expression> arguments, boolean referencesMayBeNull)
    {
        if (function.functionNullability().isReturnNullable()) {
            return true;
        }

        for (int i = 0; i < arguments.size(); i++) {
            if (!function.functionNullability().isArgumentNullable(i) && mayBeNull(plannerContext, arguments.get(i), referencesMayBeNull)) {
                return true;
            }
        }

        return false;
    }

    public static boolean mayFail(PlannerContext plannerContext, Expression expression)
    {
        return switch (expression) {
            // These expressions never fail
            case Bind _, Constant _, FieldReference _, Lambda _, Reference _ -> false;

            // These expressions need to verify their operands
            case Array e -> e.elements().stream().anyMatch(element -> mayFail(plannerContext, element));
            case Call e -> switch (matchComparison(e)) {
                case null -> mayFail(e) || e.arguments().stream().anyMatch(argument -> mayFail(plannerContext, argument));
                case Comparison comparison -> mayFail(plannerContext, comparison.left()) || mayFail(plannerContext, comparison.right());
            };
            case Case e -> e.whenClauses().stream().anyMatch(clause -> mayFail(plannerContext, clause.getOperand()) || mayFail(plannerContext, clause.getResult())) ||
                    mayFail(plannerContext, e.defaultValue());
            case Cast e -> mayFail(plannerContext, e);
            case Coalesce e -> e.operands().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case In e -> mayFail(plannerContext, e.value()) || e.valueList().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case IsNull e -> mayFail(plannerContext, e.value());
            case Let e -> mayFail(plannerContext, e.value()) || mayFail(plannerContext, e.body());
            case Logical e -> e.terms().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case Row e -> e.items().stream().anyMatch(argument -> mayFail(plannerContext, argument));
            case Match e -> mayFail(plannerContext, e.operand()) || e.clauses().stream().anyMatch(clause -> mayFail(plannerContext, clause.lambda().body()) || mayFail(plannerContext, clause.result())) ||
                    mayFail(plannerContext, e.defaultValue());
        };
    }

    // TODO: record "safety" (can the cast fail at runtime) in Cast node
    private static boolean mayFail(PlannerContext plannerContext, Cast cast)
    {
        if (mayFail(plannerContext, cast.expression())) {
            return true;
        }
        ResolvedFunction castFunction = plannerContext.getMetadata().getCoercion(cast.expression().type(), cast.type());
        if (castFunction.neverFails()) {
            return false;
        }

        TypeCoercion coercions = new TypeCoercion(plannerContext.getTypeManager()::getType, plannerContext.isLegacyVarcharToCharCoercion());
        if (coercions.canCoerce(cast.expression().type(), cast.type())) {
            return false;
        }

        return !cast.type().equals(VARCHAR);
    }

    private static boolean mayFail(Call call)
    {
        ResolvedFunction function = call.function();
        if (function.neverFails() || isDynamicFilterFunction(function.name())) {
            return false;
        }
        List<Expression> arguments = call.arguments();
        if (isModulsOrDivide(function) && arguments.get(1) instanceof Constant divisor && !canCauseDivisionByZeroError(divisor)) {
            return false;
        }
        return true;
    }

    private static boolean isModulsOrDivide(ResolvedFunction function)
    {
        return (function.name().equals(builtinFunctionName(MODULO)) || function.name().equals(builtinFunctionName(DIVIDE))) && function.signature().getArity() == 2;
    }

    private static boolean canCauseDivisionByZeroError(Constant divisor)
    {
        Object value = divisor.value();
        if (value == null) {
            return false; // dividing by null is null
        }
        return switch (divisor.type()) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _ -> (long) value == 0;
            case DecimalType decimalType -> {
                if (decimalType.isShort()) {
                    yield (long) value == 0;
                }
                yield ((Int128) value).isZero();
            }
            case NumberType _ -> switch (((TrinoNumber) value).toBigDecimal()) {
                case TrinoNumber.BigDecimalValue(BigDecimal bigdecimal) -> bigdecimal.signum() == 0;
                case TrinoNumber.Infinity _, TrinoNumber.NotANumber _ -> false;
            };
            case RealType _, DoubleType _ -> false; // will return NaN or ±Inf on division by 0
            default -> true;
        };
    }

    public static Expression not(Metadata metadata, Expression expression)
    {
        return call(
                metadata.resolveBuiltinFunction(NOT_FUNCTION_NAME, fromTypes(BOOLEAN)),
                expression);
    }
}
