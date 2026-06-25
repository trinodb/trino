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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.ResolvedFunction;
import io.trino.plugin.base.util.JsonTypeUtil;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.analyzer.TypeDescriptorTranslator;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AtLocal;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.BooleanTestPredicate;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.CompositeIntervalQualifier;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DistinctFromPredicate;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MatchPredicate;
import io.trino.sql.tree.MethodCall;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Overlay;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicate;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.QuantifiedComparisonPredicate;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleIntervalQualifier;
import io.trino.sql.tree.StaticMethodCall;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WhenClause.Operand;
import io.trino.sql.tree.WhenClause.Partial;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JsonPath2016Type;
import io.trino.type.UnknownType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.FormatFunction.FORMAT_FUNCTION_NAME;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_CATALOG_FUNCTION_NAME;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_PATH_FUNCTION_NAME;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_SCHEMA_FUNCTION_NAME;
import static io.trino.operator.scalar.SessionFunctions.CURRENT_USER_FUNCTION_NAME;
import static io.trino.operator.scalar.TryCastFunction.TRY_CAST_FUNCTION_NAME;
import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.operator.scalar.time.LocalTimeFunction.LOCALTIME_FUNCTION_NAME;
import static io.trino.operator.scalar.timestamp.LocalTimestamp.LOCALTIMESTAMP_FUNCTION_NAME;
import static io.trino.operator.scalar.timestamptz.CurrentTimestamp.CURRENT_TIMESTAMP_FUNCTION_NAME;
import static io.trino.operator.scalar.timetz.AtTimeZone.AT_TIMEZONE_FUNCTION_NAME;
import static io.trino.operator.scalar.timetz.CurrentTime.CURRENT_TIME_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.between;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.equalityClause;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrExpressions.nullIf;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.ERROR;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.KEEP;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.OMIT;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Keeps mappings of fields and AST expressions to symbols in the current plan within query boundary.
 * <p>
 * AST and IR expressions use the same class hierarchy ({@link Expression},
 * but differ in the following ways:
 * <ul>
 * <li>AST expressions contain Identifiers, while IR expressions contain SymbolReferences</li>
 * <li>FunctionCalls in AST expressions are SQL function names. In IR expressions, they contain an encoded name representing a resolved function</li>
 * </ul>
 */
public class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final Scope scope;
    private final Analysis analysis;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments;
    private final Optional<TranslationMap> outerContext;
    private final Session session;
    private final PlannerContext plannerContext;
    private final SymbolAllocator symbolAllocator;

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<ScopeAware<Expression>, Symbol> astToSymbols;
    private final Map<NodeRef<Expression>, Symbol> substitutions;
    // scope-aware mappings of relational subquery predicates (`x IN (subquery)`, `x <op> ANY
    // (subquery)`) planned by SubqueryPlanner. Keyed scope-aware on both the operand and the
    // predicate, so that structurally equal `value <predicate>` occurrences are recognized as
    // already planned across handleSubqueries calls.
    private final Map<RelationalPredicateKey, Symbol> predicateSubstitutions;

    /// Scope-aware grouping key for a relational subquery predicate: a `value <predicate>` pair
    /// where `value` is the left operand and `predicate` is an `IN (subquery)` or `<op> ANY
    /// (subquery)` node. Two pairs are equal only when both the operand and the predicate are
    /// scope-aware equivalent.
    public record RelationalPredicateKey(ScopeAware<Expression> value, ScopeAware<Predicate> predicate) {}

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Session session, PlannerContext plannerContext, SymbolAllocator symbolAllocator)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]).clone(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), session, plannerContext, symbolAllocator);
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols, Session session, PlannerContext plannerContext, SymbolAllocator symbolAllocator)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]), astToSymbols, ImmutableMap.of(), ImmutableMap.of(), session, plannerContext, symbolAllocator);
    }

    public TranslationMap(
            Optional<TranslationMap> outerContext,
            Scope scope,
            Analysis analysis,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments,
            Symbol[] fieldSymbols,
            Map<ScopeAware<Expression>, Symbol> astToSymbols,
            Map<NodeRef<Expression>, Symbol> substitutions,
            Map<RelationalPredicateKey, Symbol> predicateSubstitutions,
            Session session,
            PlannerContext plannerContext,
            SymbolAllocator symbolAllocator)
    {
        this.outerContext = requireNonNull(outerContext, "outerContext is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaArguments = requireNonNull(lambdaArguments, "lambdaArguments is null");
        this.session = requireNonNull(session, "session is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.substitutions = ImmutableMap.copyOf(substitutions);
        this.predicateSubstitutions = ImmutableMap.copyOf(requireNonNull(predicateSubstitutions, "predicateSubstitutions is null"));

        requireNonNull(fieldSymbols, "fieldSymbols is null");
        this.fieldSymbols = fieldSymbols.clone();

        requireNonNull(astToSymbols, "astToSymbols is null");
        this.astToSymbols = ImmutableMap.copyOf(astToSymbols);

        checkArgument(scope.getLocalScopeFieldCount() == fieldSymbols.length,
                "scope: %s, fields mappings: %s",
                scope.getRelationType().getAllFieldCount(),
                fieldSymbols.length);
    }

    public TranslationMap withScope(Scope scope, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields.toArray(new Symbol[0]), astToSymbols, substitutions, predicateSubstitutions, session, plannerContext, symbolAllocator);
    }

    public TranslationMap withNewMappings(Map<ScopeAware<Expression>, Symbol> mappings, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields, mappings, session, plannerContext, symbolAllocator);
    }

    public TranslationMap withAdditionalMappings(Map<ScopeAware<Expression>, Symbol> mappings)
    {
        Map<ScopeAware<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.astToSymbols);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, newMappings, substitutions, predicateSubstitutions, session, plannerContext, symbolAllocator);
    }

    public TranslationMap withAdditionalIdentityMappings(Map<NodeRef<Expression>, Symbol> mappings)
    {
        Map<NodeRef<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.substitutions);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, astToSymbols, newMappings, predicateSubstitutions, session, plannerContext, symbolAllocator);
    }

    /// Register scope-aware mappings of relational subquery predicates to their planned symbols.
    /// Used by `SubqueryPlanner` so that structurally equal predicate occurrences are recognized
    /// as already planned and not re-planned across `handleSubqueries` calls.
    public TranslationMap withAdditionalPredicateMappings(Map<RelationalPredicateKey, Symbol> mappings)
    {
        Map<RelationalPredicateKey, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.predicateSubstitutions);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, astToSymbols, substitutions, newMappings, session, plannerContext, symbolAllocator);
    }

    public List<Symbol> getFieldSymbols()
    {
        return Collections.unmodifiableList(Arrays.asList(fieldSymbols));
    }

    public Map<ScopeAware<Expression>, Symbol> getMappings()
    {
        return astToSymbols;
    }

    public Map<RelationalPredicateKey, Symbol> getPredicateMappings()
    {
        return predicateSubstitutions;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public boolean canTranslate(Expression expression)
    {
        if (astToSymbols.containsKey(scopeAwareKey(expression, analysis, scope)) ||
                substitutions.containsKey(NodeRef.of(expression)) ||
                expression instanceof io.trino.sql.tree.FieldReference) {
            return true;
        }

        if (analysis.isColumnReference(expression)) {
            ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));
            return scope.isLocalScope(field.getScope());
        }

        return false;
    }

    /// Whether the given relational `value <predicate>` pair has already been planned by
    /// `SubqueryPlanner`. The pair counts as planned when the predicate node itself, or a
    /// scope-aware equivalent `value <predicate>` occurrence, has a planned symbol. Used by
    /// `SubqueryPlanner` to skip predicates handled by a sub-plan.
    public boolean isPlanned(Expression value, Predicate predicate)
    {
        return findPlannedPredicate(value, predicate) != null;
    }

    public io.trino.sql.ir.Expression rewrite(Expression root)
    {
        verify(analysis.isAnalyzed(root), "Expression is not analyzed (%s): %s", root.getClass().getName(), root);

        return translate(root, true);
    }

    private io.trino.sql.ir.Expression translateExpression(Expression expression)
    {
        return translate(expression, false);
    }

    private io.trino.sql.ir.Expression translate(Expression expr, boolean isRoot)
    {
        Optional<Reference> mapped = tryGetMapping(expr);

        io.trino.sql.ir.Expression result;
        if (mapped.isPresent()) {
            result = mapped.get();
        }
        else {
            result = switch (expr) {
                case io.trino.sql.tree.FieldReference expression -> translate(expression);
                case Identifier expression -> translate(expression);
                case FunctionCall expression -> translate(expression);
                case StaticMethodCall expression -> translate(expression);
                case MethodCall expression -> translate(expression);
                case DereferenceExpression expression -> translate(expression);
                case Array expression -> translate(expression);
                case CurrentCatalog expression -> translate(expression);
                case CurrentSchema expression -> translate(expression);
                case CurrentPath expression -> translate(expression);
                case CurrentUser expression -> translate(expression);
                case CurrentDate expression -> translate(expression);
                case CurrentTime expression -> translate(expression);
                case CurrentTimestamp expression -> translate(expression);
                case LocalTime expression -> translate(expression);
                case LocalTimestamp expression -> translate(expression);
                case Extract expression -> translate(expression);
                case AtTimeZone expression -> translate(expression);
                case AtLocal expression -> translate(expression);
                case Format expression -> translate(expression);
                case TryExpression expression -> translate(expression);
                case Trim expression -> translate(expression);
                case Overlay expression -> translate(expression);
                case SubscriptExpression expression -> translate(expression);
                case LambdaExpression expression -> translate(expression);
                case Parameter expression -> translate(expression);
                case JsonExists expression -> translate(expression);
                case JsonValue expression -> translate(expression);
                case JsonQuery expression -> translate(expression);
                case JsonObject expression -> translate(expression);
                case JsonArray expression -> translate(expression);
                case LongLiteral expression -> translate(expression);
                case DoubleLiteral expression -> translate(expression);
                case StringLiteral expression -> translate(expression);
                case BooleanLiteral expression -> translate(expression);
                case DecimalLiteral expression -> translate(expression);
                case GenericLiteral expression -> translate(expression);
                case BinaryLiteral expression -> translate(expression);
                case IntervalLiteral expression -> translate(expression);
                case ArithmeticBinaryExpression expression -> translate(expression);
                case ArithmeticUnaryExpression expression -> translate(expression);
                case Cast expression -> translate(expression);
                case Row expression -> translate(expression);
                case NotExpression expression -> translate(expression);
                case LogicalExpression expression -> translate(expression);
                case NullLiteral _ -> new Constant(UnknownType.UNKNOWN, null);
                case CoalesceExpression expression -> translate(expression);
                case Predicated expression -> translate(expression);
                case IfExpression expression -> translate(expression);
                case SimpleCaseExpression expression -> translate(expression);
                case SearchedCaseExpression expression -> translate(expression);
                case NullIfExpression expression -> translate(expression);
                default -> throw new IllegalArgumentException("Unsupported expression (%s): %s".formatted(expr.getClass().getName(), expr));
            };
        }

        // Don't add a coercion for the top-level expression. That depends on the context
        // the expression is used and it's the responsibility of the caller.
        return isRoot ? result : QueryPlanner.coerceIfNecessary(analysis, expr, result);
    }

    private io.trino.sql.ir.Expression translate(NullIfExpression expression)
    {
        io.trino.sql.ir.Expression first = translateExpression(expression.getFirst());
        io.trino.sql.ir.Expression second = translateExpression(expression.getSecond());
        return nullIf(plannerContext.getMetadata(), symbolAllocator, first, second, analysis.getNullIfComparisonType(expression));
    }

    private io.trino.sql.ir.Expression translate(ArithmeticUnaryExpression expression)
    {
        return switch (expression.getSign()) {
            case PLUS -> translateExpression(expression.getValue());
            case MINUS -> new Call(
                    plannerContext.getMetadata().resolveOperator(OperatorType.NEGATION, ImmutableList.of(analysis.getType(expression.getValue()))),
                    ImmutableList.of(translateExpression(expression.getValue())));
        };
    }

    private io.trino.sql.ir.Expression translate(IntervalLiteral expression)
    {
        Type type = analysis.getType(expression);

        // TODO: the value should be interpreted according to the analyzed type. However, currently the analyzed type
        //       is hard-coded to either INTERVAL DAY TO SECOND or INTERVAL YEAR TO MONTH, as arbitrary precision
        //       invervals are not yet supported in the underlying type system.

        IntervalField start = switch (expression.qualifier()) {
            case SimpleIntervalQualifier simple -> simple.getField();
            case CompositeIntervalQualifier composite -> composite.getFrom();
        };

        Optional<IntervalField> end = switch (expression.qualifier()) {
            case SimpleIntervalQualifier _ -> Optional.empty();
            case CompositeIntervalQualifier composite -> Optional.of(composite.getTo());
        };

        return new Constant(
                type,
                switch (type) {
                    case IntervalDayTimeType _ -> expression.getSign().multiplier() * parseDayTimeInterval(expression.getValue(), start, end);
                    case IntervalYearMonthType _ -> expression.getSign().multiplier() * parseYearMonthInterval(expression.getValue(), start, end);
                    default -> throw new UnsupportedOperationException("Unhandled interval type: " + type);
                });
    }

    private io.trino.sql.ir.Expression translate(SearchedCaseExpression expression)
    {
        return new Case(
                expression.getWhenClauses().stream()
                        .map(clause -> new io.trino.sql.ir.WhenClause(
                                translateExpression(((Operand) clause.getMatch()).expression()),
                                translateExpression(clause.getResult())))
                        .collect(toImmutableList()),
                expression.getDefaultValue()
                        .map(this::translateExpression)
                        .orElse(new Constant(analysis.getType(expression), null)));
    }

    private io.trino.sql.ir.Expression translate(SimpleCaseExpression expression)
    {
        io.trino.sql.ir.Expression operand = translateExpression(expression.getOperand());
        Symbol parameter = symbolAllocator.newSymbol("match_operand", operand.type());
        Reference parameterReference = new Reference(parameter.type(), parameter.name());
        return new Match(
                operand,
                expression.getWhenClauses().stream()
                        .map(clause -> translateWhenClause(clause, expression.getOperand(), parameter, parameterReference))
                        .collect(toImmutableList()),
                expression.getDefaultValue()
                        .map(this::translateExpression)
                        .orElse(new Constant(analysis.getType(expression), null)));
    }

    /// Lower a simple-CASE [WhenClause] into a [MatchClause]. Bare-value
    /// WHENs become `(p) -> p = value`; extended-CASE WHENs become a lambda whose body is the
    /// boolean IR for the surrounding [Predicate] fragment, with the case operand bound once
    /// via the lambda parameter rather than re-evaluated for each clause.
    private MatchClause translateWhenClause(WhenClause clause, Expression caseOperand, Symbol parameter, Reference parameterReference)
    {
        io.trino.sql.ir.Expression result = translateExpression(clause.getResult());
        return switch (clause.getMatch()) {
            case Operand operand -> equalityClause(plannerContext.getMetadata(), parameter, translateExpression(operand.expression()), result);
            case Partial partial -> {
                io.trino.sql.ir.Expression body = translatePartialClause(caseOperand, parameterReference, partial.predicate());
                yield new MatchClause(new Lambda(ImmutableList.of(parameter), body), result);
            }
        };
    }

    /// Whether a [Predicate] is relational, i.e. its right-hand side is a subquery that
    /// `SubqueryPlanner` plans into a boolean symbol. A quantified comparison is always
    /// relational; an IN predicate is relational only when its value list is a subquery
    /// rather than an explicit [InListExpression].
    private static boolean isRelationalPredicate(Predicate predicate)
    {
        return predicate instanceof QuantifiedComparisonPredicate
                || predicate instanceof MatchPredicate
                || (predicate instanceof InPredicate in && !(in.getValueList() instanceof InListExpression));
    }

    /// Lower an extended-CASE predicate fragment to a clause body over the bound case operand.
    /// Scalar fragments translate in place via [#translatePartial]. A quantified-comparison or
    /// IN-subquery fragment is relational: `SubqueryPlanner` has already planned it into a
    /// boolean symbol, recovered here via the scope-aware lookup keyed on the case operand and
    /// the fragment.
    private io.trino.sql.ir.Expression translatePartialClause(Expression caseOperand, Reference parameterReference, Predicate fragment)
    {
        if (isRelationalPredicate(fragment)) {
            Symbol symbol = findPlannedPredicate(caseOperand, fragment);
            if (symbol == null) {
                throw new IllegalStateException("Relational extended-CASE fragment was not planned by SubqueryPlanner: " + fragment);
            }
            return symbol.toSymbolReference();
        }
        return translatePartial(parameterReference, fragment);
    }

    private io.trino.sql.ir.Expression translate(IfExpression expression)
    {
        if (expression.getFalseValue().isPresent()) {
            return ifExpression(
                    translateExpression(expression.getCondition()),
                    translateExpression(expression.getTrueValue()),
                    translateExpression(expression.getFalseValue().get()));
        }

        return ifExpression(
                translateExpression(expression.getCondition()),
                translateExpression(expression.getTrueValue()));
    }

    private io.trino.sql.ir.Expression translate(BinaryLiteral expression)
    {
        return new Constant(analysis.getType(expression), Slices.wrappedBuffer(expression.getValue()));
    }

    private io.trino.sql.ir.Expression translate(Predicated expression)
    {
        Predicate predicate = expression.getPredicate();
        if (isRelationalPredicate(predicate)) {
            Symbol symbol = findPlannedPredicate(expression.getValue(), predicate);
            if (symbol == null) {
                throw new IllegalStateException("Relational predicate was not planned by SubqueryPlanner: " + predicate);
            }
            return symbol.toSymbolReference();
        }
        io.trino.sql.ir.Expression value = translateExpression(expression.getValue());
        return translatePartial(value, predicate);
    }

    /// Resolve the symbol that `SubqueryPlanner` planned for a relational `value <predicate>`
    /// pair. The planner registers a single scope-aware mapping keyed on both the operand and the
    /// predicate, which also matches structurally equal `value <predicate>` occurrences.
    private Symbol findPlannedPredicate(Expression value, Predicate predicate)
    {
        return predicateSubstitutions.get(new RelationalPredicateKey(
                scopeAwareKey(value, analysis, scope),
                scopeAwareKey(predicate, analysis, scope)));
    }

    private io.trino.sql.ir.Expression translatePartial(io.trino.sql.ir.Expression value, Predicate clause)
    {
        return switch (clause) {
            case BetweenPredicate predicate -> {
                io.trino.sql.ir.Expression min = translateExpression(predicate.getMin());
                io.trino.sql.ir.Expression max = translateExpression(predicate.getMax());
                io.trino.sql.ir.Expression between = predicate.isSymmetric()
                        ? betweenSymmetric(value, min, max)
                        : between(plannerContext.getMetadata(), symbolAllocator, value, min, max);
                yield predicate.isNegated() ? not(plannerContext.getMetadata(), between) : between;
            }
            case ComparisonPredicate predicate -> {
                io.trino.sql.ir.Expression right = translateExpression(predicate.getRight());
                yield switch (predicate.getOperator()) {
                    case EQUAL -> comparison(plannerContext.getMetadata(), EQUAL, value, right);
                    case NOT_EQUAL -> comparison(plannerContext.getMetadata(), NOT_EQUAL, value, right);
                    case LESS_THAN -> comparison(plannerContext.getMetadata(), LESS_THAN, value, right);
                    case LESS_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, value, right);
                    case GREATER_THAN -> comparison(plannerContext.getMetadata(), GREATER_THAN, value, right);
                    case GREATER_THAN_OR_EQUAL -> comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, value, right);
                };
            }
            case DistinctFromPredicate predicate -> {
                io.trino.sql.ir.Expression right = translateExpression(predicate.getRight());
                io.trino.sql.ir.Expression identical = comparison(plannerContext.getMetadata(), IDENTICAL, value, right);
                yield predicate.isNegated() ? identical : not(plannerContext.getMetadata(), identical);
            }
            case InPredicate predicate -> {
                if (!(predicate.getValueList() instanceof InListExpression valueList)) {
                    throw new IllegalStateException("Subquery IN should have been planned by SubqueryPlanner: " + predicate.getValueList().getClass().getName());
                }
                io.trino.sql.ir.Expression in = new In(
                        value,
                        valueList.getValues().stream()
                                .map(this::translateExpression)
                                .collect(toImmutableList()));
                yield predicate.isNegated() ? not(plannerContext.getMetadata(), in) : in;
            }
            case IsNullPredicate predicate -> {
                io.trino.sql.ir.Expression isNull = new IsNull(value);
                yield predicate.isNegated() ? not(plannerContext.getMetadata(), isNull) : isNull;
            }
            case BooleanTestPredicate predicate -> {
                // value IS [NOT] TRUE/FALSE is null-safe equality against the truth constant
                // (IDENTICAL yields FALSE rather than NULL when value is NULL); IS [NOT] UNKNOWN
                // is the null test. Either way the result is a non-NULL boolean.
                io.trino.sql.ir.Expression test = switch (predicate.getTruthValue()) {
                    case TRUE -> comparison(plannerContext.getMetadata(), IDENTICAL, value, TRUE);
                    case FALSE -> comparison(plannerContext.getMetadata(), IDENTICAL, value, FALSE);
                    case UNKNOWN -> new IsNull(value);
                };
                yield predicate.isNegated() ? not(plannerContext.getMetadata(), test) : test;
            }
            case LikePredicate predicate -> translateLike(value, predicate);
            case MatchPredicate _ -> throw new IllegalStateException("MATCH predicate should have been planned by SubqueryPlanner");
            case QuantifiedComparisonPredicate _ -> throw new IllegalStateException("Quantified comparison should have been planned by SubqueryPlanner");
        };
    }

    /// Lower a `value BETWEEN SYMMETRIC min AND max` to
    /// `(value >= min AND value <= max) OR (value >= max AND value <= min)` — the bounds are an
    /// unordered pair. `value`, `min`, and `max` each appear twice, so each non-trivial operand is
    /// wrapped in a [Let] and evaluated exactly once; trivial `Reference` / `Constant` operands are
    /// inlined since duplicating them is harmless.
    private io.trino.sql.ir.Expression betweenSymmetric(io.trino.sql.ir.Expression value, io.trino.sql.ir.Expression min, io.trino.sql.ir.Expression max)
    {
        List<Symbol> symbols = new ArrayList<>();
        List<io.trino.sql.ir.Expression> bindings = new ArrayList<>();
        io.trino.sql.ir.Expression boundValue = bindIfNonTrivial(value, symbols, bindings);
        io.trino.sql.ir.Expression boundMin = bindIfNonTrivial(min, symbols, bindings);
        io.trino.sql.ir.Expression boundMax = bindIfNonTrivial(max, symbols, bindings);

        io.trino.sql.ir.Expression result = new Logical(Logical.Operator.OR, ImmutableList.of(
                new Logical(Logical.Operator.AND, ImmutableList.of(
                        comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, boundValue, boundMin),
                        comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, boundValue, boundMax))),
                new Logical(Logical.Operator.AND, ImmutableList.of(
                        comparison(plannerContext.getMetadata(), GREATER_THAN_OR_EQUAL, boundValue, boundMax),
                        comparison(plannerContext.getMetadata(), LESS_THAN_OR_EQUAL, boundValue, boundMin)))));

        for (int i = symbols.size() - 1; i >= 0; i--) {
            result = new Let(symbols.get(i), bindings.get(i), result);
        }
        return result;
    }

    private io.trino.sql.ir.Expression bindIfNonTrivial(io.trino.sql.ir.Expression expression, List<Symbol> symbols, List<io.trino.sql.ir.Expression> bindings)
    {
        if (expression instanceof Reference || expression instanceof Constant) {
            return expression;
        }
        Symbol bound = symbolAllocator.newSymbol("between", expression.type());
        symbols.add(bound);
        bindings.add(expression);
        return new Reference(expression.type(), bound.name());
    }

    private io.trino.sql.ir.Expression translate(CoalesceExpression expression)
    {
        return new Coalesce(expression.getOperands().stream()
                .map(this::translateExpression)
                .collect(toImmutableList()));
    }

    private io.trino.sql.ir.Expression translate(GenericLiteral expression)
    {
        // TODO: record the parsed values in the analyzer and pull them out here
        Type type = analysis.getType(expression);

        if (type.equals(JSON)) {
            return new Constant(type, JsonTypeUtil.jsonParse(utf8Slice(expression.getValue())));
        }

        InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
        ResolvedFunction resolvedFunction = plannerContext.getMetadata().getCoercion(VARCHAR, type);
        Object value = functionInvoker.invoke(resolvedFunction, session.toConnectorSession(), ImmutableList.of(utf8Slice(expression.getValue())));

        return new Constant(type, value);
    }

    private io.trino.sql.ir.Expression translate(DecimalLiteral expression)
    {
        DecimalType type = (DecimalType) analysis.getType(expression);

        // TODO: record the parsed values in the analyzer and pull them out here
        DecimalParseResult parsed = Decimals.parse(expression.getValue());
        checkState(parsed.getType().equals(type));

        return new Constant(type, parsed.getObject());
    }

    private io.trino.sql.ir.Expression translate(LogicalExpression expression)
    {
        return new Logical(
                switch (expression.getOperator()) {
                    case AND -> Logical.Operator.AND;
                    case OR -> Logical.Operator.OR;
                },
                expression.getTerms().stream()
                        .map(this::translateExpression)
                        .collect(toImmutableList()));
    }

    private io.trino.sql.ir.Expression translate(BooleanLiteral expression)
    {
        if (expression.equals(BooleanLiteral.TRUE_LITERAL)) {
            return TRUE;
        }

        if (expression.equals(BooleanLiteral.FALSE_LITERAL)) {
            return FALSE;
        }

        throw new IllegalArgumentException("Unknown boolean literal: " + expression);
    }

    private io.trino.sql.ir.Expression translate(NotExpression expression)
    {
        return not(plannerContext.getMetadata(), translateExpression(expression.getValue()));
    }

    private io.trino.sql.ir.Expression translate(Row expression)
    {
        return new io.trino.sql.ir.Row(
                expression.getFields().stream()
                        .map(Row.Field::getExpression)
                        .map(this::translateExpression)
                        .collect(toImmutableList()),
                (RowType) analysis.getType(expression));
    }

    private io.trino.sql.ir.Expression translate(Cast expression)
    {
        if (expression.isSafe()) {
            return new Call(
                    plannerContext.getMetadata().getCoercion(
                            builtinFunctionName(TRY_CAST_FUNCTION_NAME),
                            analysis.getType(expression.getExpression()),
                            analysis.getType(expression)),
                    ImmutableList.of(translateExpression(expression.getExpression())));
        }

        return new io.trino.sql.ir.Cast(
                translateExpression(expression.getExpression()),
                analysis.getType(expression));
    }

    private io.trino.sql.ir.Expression translate(DoubleLiteral expression)
    {
        return new Constant(DOUBLE, expression.getValue());
    }

    private io.trino.sql.ir.Expression translate(ArithmeticBinaryExpression expression)
    {
        OperatorType operatorType = switch (expression.getOperator()) {
            case ADD -> OperatorType.ADD;
            case SUBTRACT -> OperatorType.SUBTRACT;
            case MULTIPLY -> OperatorType.MULTIPLY;
            case DIVIDE -> OperatorType.DIVIDE;
            case MODULO -> OperatorType.MODULO;
        };

        return new Call(
                plannerContext.getMetadata().resolveOperator(operatorType, ImmutableList.of(getCoercedType(expression.getLeft()), getCoercedType(expression.getRight()))),
                ImmutableList.of(
                        translateExpression(expression.getLeft()),
                        translateExpression(expression.getRight())));
    }

    private Type getCoercedType(Expression left)
    {
        Type leftType = analysis.getCoercion(left);
        if (leftType == null) {
            leftType = analysis.getType(left);
        }
        return leftType;
    }

    private io.trino.sql.ir.Expression translate(StringLiteral expression)
    {
        return new Constant(analysis.getType(expression), utf8Slice(expression.getValue()));
    }

    private io.trino.sql.ir.Expression translate(LongLiteral expression)
    {
        return new Constant(analysis.getType(expression), expression.getParsedValue());
    }

    private io.trino.sql.ir.Expression translate(io.trino.sql.tree.FieldReference expression)
    {
        return getSymbolForColumn(expression)
                .map(Symbol::toSymbolReference)
                .orElseThrow(() -> new IllegalStateException(format("No symbol mapping for node '%s' (%s)", expression, expression.getFieldIndex())));
    }

    private io.trino.sql.ir.Expression translate(Identifier expression)
    {
        LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(expression);
        if (referencedLambdaArgumentDeclaration != null) {
            Symbol symbol = lambdaArguments.get(NodeRef.of(referencedLambdaArgumentDeclaration));
            return symbol.toSymbolReference();
        }

        return getSymbolForColumn(expression)
                .map(Symbol::toSymbolReference)
                .get();
    }

    private io.trino.sql.ir.Expression translate(FunctionCall expression)
    {
        if (analysis.isPatternNavigationFunction(expression)) {
            return translate(expression.getArguments().getFirst().getValue(), false);
        }

        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(expression);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", expression);

        // Emit arguments in resolved signature order: for positional calls the binding
        // is the identity; for named calls it reorders so values land at their
        // declared positions.
        List<CallArgument> arguments = expression.getArguments();
        List<io.trino.sql.ir.Expression> translated = analysis.getArgumentBinding(expression).stream()
                .map(arguments::get)
                .map(CallArgument::getValue)
                .map(this::translateExpression)
                .collect(toImmutableList());

        Optional<Identifier> methodReceiver = analysis.getMethodCallReceiver(expression);
        if (methodReceiver.isPresent()) {
            return new Call(
                    resolvedFunction.get(),
                    ImmutableList.<io.trino.sql.ir.Expression>builder()
                            .add(translateExpression(methodReceiver.get()))
                            .addAll(translated)
                            .build());
        }

        return new Call(resolvedFunction.get(), translated);
    }

    private io.trino.sql.ir.Expression translate(StaticMethodCall expression)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(expression);
        checkArgument(resolvedFunction.isPresent(), "Static method has not been analyzed: %s", expression);

        return new Call(resolvedFunction.get(), translateMethodArguments(expression, expression.getArguments()));
    }

    private io.trino.sql.ir.Expression translate(MethodCall expression)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(expression);
        checkArgument(resolvedFunction.isPresent(), "Method has not been analyzed: %s", expression);

        return new Call(
                resolvedFunction.get(),
                ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(translateExpression(expression.getReceiver()))
                        .addAll(translateMethodArguments(expression, expression.getArguments()))
                        .build());
    }

    // Emit arguments in resolved signature order: for positional calls the binding
    // is the identity; for named calls it reorders so values land at their
    // declared positions.
    private List<io.trino.sql.ir.Expression> translateMethodArguments(Expression methodCall, List<CallArgument> arguments)
    {
        return analysis.getArgumentBinding(methodCall).stream()
                .map(arguments::get)
                .map(CallArgument::getValue)
                .map(this::translateExpression)
                .collect(toImmutableList());
    }

    private io.trino.sql.ir.Expression translate(DereferenceExpression expression)
    {
        if (analysis.isColumnReference(expression)) {
            return getSymbolForColumn(expression)
                    .map(Symbol::toSymbolReference)
                    .orElseThrow(() -> new IllegalStateException(format("No mapping for %s", expression)));
        }

        RowType rowType = (RowType) analysis.getType(expression.getBase());
        String fieldName = expression.getField().orElseThrow().getValue();

        List<RowType.Field> fields = rowType.getFields();
        int index = -1;
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                index = i;
            }
        }

        checkState(index >= 0, "could not find field name: %s", fieldName);

        return new FieldReference(translateExpression(expression.getBase()), index);
    }

    private io.trino.sql.ir.Expression translate(Array expression)
    {
        List<io.trino.sql.ir.Expression> values = expression.getValues().stream()
                .map(this::translateExpression)
                .collect(toImmutableList());

        Type type = analysis.getType(expression);
        return new io.trino.sql.ir.Array(((ArrayType) type).getElementType(), values);
    }

    private io.trino.sql.ir.Expression translate(CurrentCatalog unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction(CURRENT_CATALOG_FUNCTION_NAME, ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentSchema unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction(CURRENT_SCHEMA_FUNCTION_NAME, ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentPath unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction(CURRENT_PATH_FUNCTION_NAME, ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentUser unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction(CURRENT_USER_FUNCTION_NAME, ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentDate unused)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName("current_date")
                .build();
    }

    private io.trino.sql.ir.Expression translate(CurrentTime node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(CURRENT_TIME_FUNCTION_NAME)
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(CurrentTimestamp node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(CURRENT_TIMESTAMP_FUNCTION_NAME)
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(LocalTime node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(LOCALTIME_FUNCTION_NAME)
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(LocalTimestamp node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(LOCALTIMESTAMP_FUNCTION_NAME)
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(Extract node)
    {
        io.trino.sql.ir.Expression value = translateExpression(node.getExpression());
        Type type = analysis.getType(node.getExpression());

        return switch (node.getField()) {
            case YEAR -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("year")
                    .addArgument(type, value)
                    .build();
            case QUARTER -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("quarter")
                    .addArgument(type, value)
                    .build();
            case MONTH -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("month")
                    .addArgument(type, value)
                    .build();
            case WEEK -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("week")
                    .addArgument(type, value)
                    .build();
            case DAY, DAY_OF_MONTH -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("day")
                    .addArgument(type, value)
                    .build();
            case DAY_OF_WEEK, DOW -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("day_of_week")
                    .addArgument(type, value)
                    .build();
            case DAY_OF_YEAR, DOY -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("day_of_year")
                    .addArgument(type, value)
                    .build();
            case YEAR_OF_WEEK, YOW -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("year_of_week")
                    .addArgument(type, value)
                    .build();
            case HOUR -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("hour")
                    .addArgument(type, value)
                    .build();
            case MINUTE -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("minute")
                    .addArgument(type, value)
                    .build();
            case SECOND -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("second")
                    .addArgument(type, value)
                    .build();
            case TIMEZONE_MINUTE -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("timezone_minute")
                    .addArgument(type, value)
                    .build();
            case TIMEZONE_HOUR -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("timezone_hour")
                    .addArgument(type, value)
                    .build();
        };
    }

    private io.trino.sql.ir.Expression translate(AtTimeZone node)
    {
        return atTimeZone(
                analysis.getType(node.getValue()),
                translateExpression(node.getValue()),
                analysis.getType(node.getTimeZone()),
                translateExpression(node.getTimeZone()));
    }

    private io.trino.sql.ir.Expression translate(AtLocal node)
    {
        String zoneId = session.getTimeZoneKey().getId();
        Type timeZoneType = createVarcharType(zoneId.length());
        return atTimeZone(
                analysis.getType(node.getValue()),
                translateExpression(node.getValue()),
                timeZoneType,
                new Constant(timeZoneType, utf8Slice(zoneId)));
    }

    private io.trino.sql.ir.Expression atTimeZone(Type valueType, io.trino.sql.ir.Expression value, Type timeZoneType, io.trino.sql.ir.Expression timeZone)
    {
        return switch (valueType) {
            case TimeType type -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName(AT_TIMEZONE_FUNCTION_NAME)
                    .addArgument(createTimeWithTimeZoneType(type.getPrecision()), new io.trino.sql.ir.Cast(value, createTimeWithTimeZoneType(type.getPrecision())))
                    .addArgument(timeZoneType, timeZone)
                    .build();
            case TimeWithTimeZoneType _ -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName(AT_TIMEZONE_FUNCTION_NAME)
                    .addArgument(valueType, value)
                    .addArgument(timeZoneType, timeZone)
                    .build();
            case TimestampType type -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("at_timezone")
                    .addArgument(createTimestampWithTimeZoneType(type.getPrecision()), new io.trino.sql.ir.Cast(value, createTimestampWithTimeZoneType(type.getPrecision())))
                    .addArgument(timeZoneType, timeZone)
                    .build();
            case TimestampWithTimeZoneType _ -> BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("at_timezone")
                    .addArgument(valueType, value)
                    .addArgument(timeZoneType, timeZone)
                    .build();
            default -> throw new IllegalArgumentException("Unexpected type: " + valueType);
        };
    }

    private io.trino.sql.ir.Expression translate(Format node)
    {
        List<io.trino.sql.ir.Expression> arguments = node.getArguments().stream()
                .map(this::translateExpression)
                .collect(toImmutableList());
        List<Type> argumentTypes = node.getArguments().stream()
                .map(analysis::getType)
                .collect(toImmutableList());

        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(FORMAT_FUNCTION_NAME)
                .addArgument(VARCHAR, new io.trino.sql.ir.Cast(arguments.get(0), VARCHAR))
                .addArgument(RowType.anonymous(argumentTypes.subList(1, arguments.size())), new io.trino.sql.ir.Row(arguments.subList(1, arguments.size())))
                .build();
    }

    private io.trino.sql.ir.Expression translate(TryExpression node)
    {
        Type type = analysis.getType(node);
        io.trino.sql.ir.Expression expression = translateExpression(node.getInnerExpression());

        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(TRY_FUNCTION_NAME)
                .addArgument(new FunctionType(ImmutableList.of(), type), new Lambda(ImmutableList.of(), expression))
                .build();
    }

    private io.trino.sql.ir.Expression translateLike(io.trino.sql.ir.Expression value, LikePredicate predicate)
    {
        io.trino.sql.ir.Expression pattern = translateExpression(predicate.getPattern());
        Optional<io.trino.sql.ir.Expression> escape = predicate.getEscape().map(this::translateExpression);

        Call patternCall;
        if (escape.isPresent()) {
            patternCall = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName(LIKE_PATTERN_FUNCTION_NAME)
                    .addArgument(VARCHAR, new io.trino.sql.ir.Cast(pattern, VARCHAR))
                    .addArgument(VARCHAR, new io.trino.sql.ir.Cast(escape.get(), VARCHAR))
                    .build();
        }
        else {
            patternCall = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName(LIKE_PATTERN_FUNCTION_NAME)
                    .addArgument(VARCHAR, new io.trino.sql.ir.Cast(pattern, VARCHAR))
                    .build();
        }

        io.trino.sql.ir.Expression like = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(LIKE_FUNCTION_NAME)
                .addArgument(value.type(), value)
                .addArgument(LIKE_PATTERN, patternCall)
                .build();
        return predicate.isNegated() ? not(plannerContext.getMetadata(), like) : like;
    }

    private io.trino.sql.ir.Expression translate(Trim node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.builder();
        arguments.add(translateExpression(node.getTrimSource()));
        node.getTrimCharacter()
                .map(this::translateExpression)
                .ifPresent(arguments::add);

        return new Call(resolvedFunction.get(), arguments.build());
    }

    private io.trino.sql.ir.Expression translate(Overlay node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.builder();
        arguments.add(translateExpression(node.getValue()));
        arguments.add(translateExpression(node.getReplacement()));
        arguments.add(translateExpression(node.getStart()));
        node.getLength()
                .map(this::translateExpression)
                .ifPresent(arguments::add);

        return new Call(resolvedFunction.get(), arguments.build());
    }

    private io.trino.sql.ir.Expression translate(SubscriptExpression node)
    {
        Type baseType = analysis.getType(node.getBase());
        if (baseType instanceof RowType) {
            // Do not rewrite subscript index into symbol. Row subscript index is required to be a literal.
            io.trino.sql.ir.Expression rewrittenBase = translateExpression(node.getBase());
            LongLiteral index = (LongLiteral) node.getIndex();
            return new FieldReference(
                    rewrittenBase, toIntExact(index.getParsedValue() - 1));
        }

        ResolvedFunction operator = plannerContext.getMetadata()
                .resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(getCoercedType(node.getBase()), getCoercedType(node.getIndex())));

        return new Call(
                operator,
                ImmutableList.of(
                        new io.trino.sql.ir.Cast(translateExpression(node.getBase()), operator.signature().getArgumentType(0)),
                        new io.trino.sql.ir.Cast(translateExpression(node.getIndex()), operator.signature().getArgumentType(1))));
    }

    private io.trino.sql.ir.Expression translate(LambdaExpression node)
    {
        checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");

        ImmutableList.Builder<Symbol> newArguments = ImmutableList.builder();
        for (LambdaArgumentDeclaration argument : node.getArguments()) {
            newArguments.add(lambdaArguments.get(NodeRef.of(argument)));
        }
        io.trino.sql.ir.Expression rewrittenBody = translateExpression(node.getBody());
        return new Lambda(newArguments.build(), rewrittenBody);
    }

    private io.trino.sql.ir.Expression translate(Parameter node)
    {
        checkState(analysis.getParameters().size() > node.getId(), "Too few parameter values");
        return translateExpression(analysis.getParameters().get(NodeRef.of(node)));
    }

    private io.trino.sql.ir.Expression translate(JsonExists node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        //  apply the input function to the input expression
        Constant failOnError = new Constant(BOOLEAN, node.getErrorBehavior() == JsonExists.ErrorBehavior.ERROR);
        ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
        io.trino.sql.ir.Expression input = new Call(inputToJson, ImmutableList.of(
                translateExpression(node.getJsonPathInvocation().getInputExpression()),
                failOnError));

        // apply the input functions to the JSON path parameters having FORMAT,
        // and collect all JSON path parameters in a Row
        ParametersRow orderedParameters = getParametersRow(
                node.getJsonPathInvocation().getPathParameters(),
                node.getJsonPathInvocation().getPathParameters().stream()
                        .map(parameter -> translateExpression(parameter.getParameter()))
                        .toList(),
                resolvedFunction.get().signature().getArgumentType(2),
                failOnError);

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.parametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(new TypeDescriptor(JsonPath2016Type.NAME)), path);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.parametersRow())
                .add(new Constant(TINYINT, (long) node.getErrorBehavior().ordinal()));

        return new Call(resolvedFunction.get(), arguments.build());
    }

    private io.trino.sql.ir.Expression translate(JsonValue node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        //  apply the input function to the input expression
        Constant failOnError = new Constant(BOOLEAN, node.getErrorBehavior() == JsonValue.EmptyOrErrorBehavior.ERROR);
        ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
        io.trino.sql.ir.Expression input = new Call(inputToJson, ImmutableList.of(
                translateExpression(node.getJsonPathInvocation().getInputExpression()),
                failOnError));

        // apply the input functions to the JSON path parameters having FORMAT,
        // and collect all JSON path parameters in a Row
        ParametersRow orderedParameters = getParametersRow(
                node.getJsonPathInvocation().getPathParameters(),
                node.getJsonPathInvocation().getPathParameters().stream()
                        .map(parameter -> translateExpression(parameter.getParameter()))
                        .toList(),
                resolvedFunction.get().signature().getArgumentType(2),
                failOnError);

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.parametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(new TypeDescriptor(JsonPath2016Type.NAME)), path);
        Type returnType = resolvedFunction.get().signature().getReturnType();

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.parametersRow())
                .add(new Constant(returnType, null))
                .add(new Constant(TINYINT, (long) node.getEmptyBehavior().ordinal()))
                .add(node.getEmptyDefault()
                        .map(this::translateExpression)
                        .map(expression -> new Lambda(ImmutableList.of(), expression))
                        .orElseGet(() -> new Lambda(
                                ImmutableList.of(),
                                new Constant(((FunctionType) resolvedFunction.get().signature().getArgumentType(5)).getReturnType(), null))))
                .add(new Constant(TINYINT, (long) node.getErrorBehavior().ordinal()))
                .add(node.getErrorDefault()
                        .map(this::translateExpression)
                        .map(expression -> new Lambda(ImmutableList.of(), expression))
                        .orElseGet(() -> new Lambda(
                                ImmutableList.of(),
                                new Constant(((FunctionType) resolvedFunction.get().signature().getArgumentType(7)).getReturnType(), null))));

        return new Call(resolvedFunction.get(), arguments.build());
    }

    private io.trino.sql.ir.Expression translate(JsonQuery node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        //  apply the input function to the input expression
        Constant failOnError = new Constant(BOOLEAN, node.getErrorBehavior() == ERROR);
        ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
        io.trino.sql.ir.Expression input = new Call(inputToJson, ImmutableList.of(
                translateExpression(node.getJsonPathInvocation().getInputExpression()),
                failOnError));

        // apply the input functions to the JSON path parameters having FORMAT,
        // and collect all JSON path parameters in a Row
        ParametersRow orderedParameters = getParametersRow(
                node.getJsonPathInvocation().getPathParameters(),
                node.getJsonPathInvocation().getPathParameters().stream()
                        .map(parameter -> translateExpression(parameter.getParameter()))
                        .toList(),
                resolvedFunction.get().signature().getArgumentType(2),
                failOnError);

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.parametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(new TypeDescriptor(JsonPath2016Type.NAME)), path);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.parametersRow())
                .add(new Constant(TINYINT, (long) node.getWrapperBehavior().ordinal()))
                .add(new Constant(TINYINT, (long) node.getEmptyBehavior().ordinal()))
                .add(new Constant(TINYINT, (long) node.getErrorBehavior().ordinal()));

        io.trino.sql.ir.Expression function = new Call(resolvedFunction.get(), arguments.build());

        // apply function to format output
        Constant errorBehavior = new Constant(TINYINT, (long) node.getErrorBehavior().ordinal());
        Constant omitQuotes = new Constant(BOOLEAN, node.getQuotesBehavior().orElse(KEEP) == OMIT);
        ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
        io.trino.sql.ir.Expression result = new Call(outputFunction, ImmutableList.of(function, errorBehavior, omitQuotes));

        // cast to requested returned type
        Type returnedType = node.getReturnedType()
                .map(TypeDescriptorTranslator::toTypeDescriptor)
                .map(plannerContext.getTypeManager()::getType)
                .orElse(VARCHAR);

        Type resultType = outputFunction.signature().getReturnType();
        if (!resultType.equals(returnedType)) {
            result = new io.trino.sql.ir.Cast(result, returnedType);
        }

        return result;
    }

    private io.trino.sql.ir.Expression translate(JsonObject node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        io.trino.sql.ir.Expression keysRow;
        io.trino.sql.ir.Expression valuesRow;

        // prepare keys and values as rows
        if (node.getMembers().isEmpty()) {
            checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.get().signature().getArgumentType(0)));
            checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.get().signature().getArgumentType(1)));
            keysRow = new Constant(JSON_NO_PARAMETERS_ROW_TYPE, null);
            valuesRow = new Constant(JSON_NO_PARAMETERS_ROW_TYPE, null);
        }
        else {
            ImmutableList.Builder<io.trino.sql.ir.Expression> keys = ImmutableList.builder();
            ImmutableList.Builder<io.trino.sql.ir.Expression> values = ImmutableList.builder();
            for (JsonObjectMember member : node.getMembers()) {
                Expression value = member.getValue();

                io.trino.sql.ir.Expression rewrittenKey = translateExpression(member.getKey());
                keys.add(rewrittenKey);

                io.trino.sql.ir.Expression rewrittenValue = translateExpression(value);
                ResolvedFunction valueToJson = analysis.getJsonInputFunction(value);
                if (valueToJson != null) {
                    values.add(new Call(valueToJson, ImmutableList.of(rewrittenValue, TRUE)));
                }
                else {
                    values.add(rewrittenValue);
                }
            }
            keysRow = new io.trino.sql.ir.Row(keys.build());
            valuesRow = new io.trino.sql.ir.Row(values.build());
        }

        List<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(keysRow)
                .add(valuesRow)
                .add(node.isNullOnNull() ? TRUE : FALSE)
                .add(node.isUniqueKeys() ? TRUE : FALSE)
                .build();

        io.trino.sql.ir.Expression function = new Call(resolvedFunction.get(), arguments);

        // apply function to format output
        ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
        io.trino.sql.ir.Expression result = new Call(outputFunction, ImmutableList.of(
                function,
                new Constant(TINYINT, (long) ERROR.ordinal()),
                FALSE));

        // cast to requested returned type
        Type returnedType = node.getReturnedType()
                .map(TypeDescriptorTranslator::toTypeDescriptor)
                .map(plannerContext.getTypeManager()::getType)
                .orElse(VARCHAR);

        Type resultType = outputFunction.signature().getReturnType();
        if (!resultType.equals(returnedType)) {
            result = new io.trino.sql.ir.Cast(result, returnedType);
        }

        return result;
    }

    private io.trino.sql.ir.Expression translate(JsonArray node)
    {
        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(node);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", node);

        io.trino.sql.ir.Expression elementsRow;

        // prepare elements as row
        if (node.getElements().isEmpty()) {
            checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.get().signature().getArgumentType(0)));
            elementsRow = new Constant(JSON_NO_PARAMETERS_ROW_TYPE, null);
        }
        else {
            ImmutableList.Builder<io.trino.sql.ir.Expression> elements = ImmutableList.builder();
            for (JsonArrayElement arrayElement : node.getElements()) {
                Expression element = arrayElement.getValue();
                io.trino.sql.ir.Expression rewrittenElement = translateExpression(element);
                ResolvedFunction elementToJson = analysis.getJsonInputFunction(element);
                if (elementToJson != null) {
                    elements.add(new Call(elementToJson, ImmutableList.of(rewrittenElement, TRUE)));
                }
                else {
                    elements.add(rewrittenElement);
                }
            }
            elementsRow = new io.trino.sql.ir.Row(elements.build());
        }

        List<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(elementsRow)
                .add(node.isNullOnNull() ? TRUE : FALSE)
                .build();

        io.trino.sql.ir.Expression function = new Call(resolvedFunction.get(), arguments);

        // apply function to format output
        ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
        io.trino.sql.ir.Expression result = new Call(outputFunction, ImmutableList.of(
                function,
                new Constant(TINYINT, (long) ERROR.ordinal()),
                FALSE));

        // cast to requested returned type
        Type returnedType = node.getReturnedType()
                .map(TypeDescriptorTranslator::toTypeDescriptor)
                .map(plannerContext.getTypeManager()::getType)
                .orElse(VARCHAR);

        Type resultType = outputFunction.signature().getReturnType();
        if (!resultType.equals(returnedType)) {
            result = new io.trino.sql.ir.Cast(result, returnedType);
        }

        return result;
    }

    private Optional<Reference> tryGetMapping(Expression expression)
    {
        Symbol symbol = substitutions.get(NodeRef.of(expression));
        if (symbol == null) {
            symbol = astToSymbols.get(scopeAwareKey(expression, analysis, scope));
        }

        return Optional.ofNullable(symbol)
                .map(Symbol::toSymbolReference);
    }

    private Optional<Symbol> getSymbolForColumn(Expression expression)
    {
        if (!analysis.isColumnReference(expression)) {
            // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda argument reference).
            // In such case, the expression might still be resolvable with plan.getScope() but we should not resolve it.
            return Optional.empty();
        }

        ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));

        if (scope.isLocalScope(field.getScope())) {
            return Optional.of(fieldSymbols[field.getHierarchyFieldIndex()]);
        }

        if (outerContext.isPresent()) {
            return Optional.of(Symbol.from(outerContext.get().rewrite(expression)));
        }

        return Optional.empty();
    }

    public Scope getScope()
    {
        return scope;
    }

    public ParametersRow getParametersRow(
            List<JsonPathParameter> pathParameters,
            List<io.trino.sql.ir.Expression> rewrittenPathParameters,
            Type parameterRowType,
            Constant failOnError)
    {
        io.trino.sql.ir.Expression parametersRow;
        List<String> parametersOrder;
        if (!pathParameters.isEmpty()) {
            ImmutableList.Builder<io.trino.sql.ir.Expression> parameters = ImmutableList.builder();
            for (int i = 0; i < pathParameters.size(); i++) {
                ResolvedFunction parameterToJson = analysis.getJsonInputFunction(pathParameters.get(i).getParameter());
                io.trino.sql.ir.Expression rewrittenParameter = rewrittenPathParameters.get(i);
                if (parameterToJson != null) {
                    parameters.add(new Call(parameterToJson, ImmutableList.of(rewrittenParameter, failOnError)));
                }
                else {
                    parameters.add(rewrittenParameter);
                }
            }
            parametersRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.Row(parameters.build()), parameterRowType);
            parametersOrder = pathParameters.stream()
                    .map(parameter -> parameter.getName().getCanonicalValue())
                    .collect(toImmutableList());
        }
        else {
            checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(parameterRowType), "invalid type of parameters row when no parameters are passed");
            parametersRow = new Constant(JSON_NO_PARAMETERS_ROW_TYPE, null);
            parametersOrder = ImmutableList.of();
        }

        return new ParametersRow(parametersRow, parametersOrder);
    }

    public record ParametersRow(io.trino.sql.ir.Expression parametersRow, List<String> parametersOrder)
    {
        public ParametersRow
        {
            requireNonNull(parametersRow, "parametersRow is null");
            requireNonNull(parametersOrder, "parametersOrder is null");
        }
    }
}
