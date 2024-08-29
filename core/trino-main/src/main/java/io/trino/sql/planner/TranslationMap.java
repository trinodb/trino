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
import io.trino.operator.scalar.FormatFunction;
import io.trino.operator.scalar.TryFunction;
import io.trino.plugin.base.util.JsonTypeUtil;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
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
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
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
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.type.FunctionType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JsonPath2016Type;
import io.trino.type.UnknownType;

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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.not;
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

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<ScopeAware<Expression>, Symbol> astToSymbols;
    private final Map<NodeRef<Expression>, Symbol> substitutions;

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Session session, PlannerContext plannerContext)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]).clone(), ImmutableMap.of(), ImmutableMap.of(), session, plannerContext);
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols, Session session, PlannerContext plannerContext)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]), astToSymbols, ImmutableMap.of(), session, plannerContext);
    }

    public TranslationMap(
            Optional<TranslationMap> outerContext,
            Scope scope,
            Analysis analysis,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments,
            Symbol[] fieldSymbols,
            Map<ScopeAware<Expression>, Symbol> astToSymbols,
            Map<NodeRef<Expression>, Symbol> substitutions,
            Session session,
            PlannerContext plannerContext)
    {
        this.outerContext = requireNonNull(outerContext, "outerContext is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaArguments = requireNonNull(lambdaArguments, "lambdaArguments is null");
        this.session = requireNonNull(session, "session is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.substitutions = ImmutableMap.copyOf(substitutions);

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
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields.toArray(new Symbol[0]), astToSymbols, substitutions, session, plannerContext);
    }

    public TranslationMap withNewMappings(Map<ScopeAware<Expression>, Symbol> mappings, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields, mappings, session, plannerContext);
    }

    public TranslationMap withAdditionalMappings(Map<ScopeAware<Expression>, Symbol> mappings)
    {
        Map<ScopeAware<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.astToSymbols);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, newMappings, substitutions, session, plannerContext);
    }

    public TranslationMap withAdditionalIdentityMappings(Map<NodeRef<Expression>, Symbol> mappings)
    {
        Map<NodeRef<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.substitutions);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, astToSymbols, newMappings, session, plannerContext);
    }

    public List<Symbol> getFieldSymbols()
    {
        return Collections.unmodifiableList(Arrays.asList(fieldSymbols));
    }

    public Map<ScopeAware<Expression>, Symbol> getMappings()
    {
        return astToSymbols;
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
                case Format expression -> translate(expression);
                case TryExpression expression -> translate(expression);
                case LikePredicate expression -> translate(expression);
                case Trim expression -> translate(expression);
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
                case ComparisonExpression expression -> translate(expression);
                case Cast expression -> translate(expression);
                case Row expression -> translate(expression);
                case NotExpression expression -> translate(expression);
                case LogicalExpression expression -> translate(expression);
                case NullLiteral expression -> new Constant(UnknownType.UNKNOWN, null);
                case CoalesceExpression expression -> translate(expression);
                case IsNullPredicate expression -> translate(expression);
                case IsNotNullPredicate expression -> translate(expression);
                case BetweenPredicate expression -> translate(expression);
                case IfExpression expression -> translate(expression);
                case InPredicate expression -> translate(expression);
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
        return new NullIf(
                translateExpression(expression.getFirst()),
                translateExpression(expression.getSecond()));
    }

    private io.trino.sql.ir.Expression translate(ArithmeticUnaryExpression expression)
    {
        return switch (expression.getSign()) {
            case PLUS -> translateExpression(expression.getValue());
            case MINUS -> new io.trino.sql.ir.Call(
                    plannerContext.getMetadata().resolveOperator(OperatorType.NEGATION, ImmutableList.of(analysis.getType(expression.getValue()))),
                    ImmutableList.of(translateExpression(expression.getValue())));
        };
    }

    private io.trino.sql.ir.Expression translate(IntervalLiteral expression)
    {
        Type type = analysis.getType(expression);

        return new Constant(
                type,
                switch (type) {
                    case IntervalYearMonthType t -> expression.getSign().multiplier() * parseYearMonthInterval(expression.getValue(), expression.getStartField(), expression.getEndField());
                    case IntervalDayTimeType t -> expression.getSign().multiplier() * parseDayTimeInterval(expression.getValue(), expression.getStartField(), expression.getEndField());
                    default -> throw new IllegalArgumentException("Unexpected type for IntervalLiteral: %s" + type);
                });
    }

    private io.trino.sql.ir.Expression translate(SearchedCaseExpression expression)
    {
        return new Case(
                expression.getWhenClauses().stream()
                        .map(clause -> new io.trino.sql.ir.WhenClause(
                                translateExpression(clause.getOperand()),
                                translateExpression(clause.getResult())))
                        .collect(toImmutableList()),
                expression.getDefaultValue()
                        .map(this::translateExpression)
                        .orElse(new Constant(analysis.getType(expression), null)));
    }

    private io.trino.sql.ir.Expression translate(SimpleCaseExpression expression)
    {
        return new Switch(
                translateExpression(expression.getOperand()),
                expression.getWhenClauses().stream()
                        .map(clause -> new io.trino.sql.ir.WhenClause(
                                translateExpression(clause.getOperand()),
                                translateExpression(clause.getResult())))
                        .collect(toImmutableList()),
                expression.getDefaultValue()
                        .map(this::translateExpression)
                        .orElse(new Constant(analysis.getType(expression), null)));
    }

    private io.trino.sql.ir.Expression translate(InPredicate expression)
    {
        return new In(
                translateExpression(expression.getValue()),
                ((InListExpression) expression.getValueList()).getValues().stream()
                        .map(this::translateExpression)
                        .collect(toImmutableList()));
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

    private io.trino.sql.ir.Expression translate(BetweenPredicate expression)
    {
        return new Between(
                translateExpression(expression.getValue()),
                translateExpression(expression.getMin()),
                translateExpression(expression.getMax()));
    }

    private io.trino.sql.ir.Expression translate(IsNullPredicate expression)
    {
        return new IsNull(translateExpression(expression.getValue()));
    }

    private io.trino.sql.ir.Expression translate(IsNotNullPredicate expression)
    {
        return not(
                plannerContext.getMetadata(),
                new IsNull(
                        translateExpression(expression.getValue())));
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
        return new io.trino.sql.ir.Row(expression.getItems().stream()
                .map(this::translateExpression)
                .collect(toImmutableList()));
    }

    private io.trino.sql.ir.Expression translate(ComparisonExpression expression)
    {
        io.trino.sql.ir.Expression left = translateExpression(expression.getLeft());
        io.trino.sql.ir.Expression right = translateExpression(expression.getRight());

        return switch (expression.getOperator()) {
            case EQUAL -> new Comparison(EQUAL, left, right);
            case NOT_EQUAL -> new Comparison(NOT_EQUAL, left, right);
            case LESS_THAN -> new Comparison(LESS_THAN, left, right);
            case LESS_THAN_OR_EQUAL -> new Comparison(LESS_THAN_OR_EQUAL, left, right);
            case GREATER_THAN -> new Comparison(GREATER_THAN, left, right);
            case GREATER_THAN_OR_EQUAL -> new Comparison(GREATER_THAN_OR_EQUAL, left, right);
            case IS_DISTINCT_FROM -> not(plannerContext.getMetadata(), new Comparison(IDENTICAL, left, right));
        };
    }

    private io.trino.sql.ir.Expression translate(Cast expression)
    {
        if (expression.isSafe()) {
            return new Call(
                    plannerContext.getMetadata().getCoercion(
                            builtinFunctionName("$try_cast"),
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
            case MODULUS -> OperatorType.MODULUS;
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
            return translate(expression.getArguments().getFirst(), false);
        }

        Optional<ResolvedFunction> resolvedFunction = analysis.getResolvedFunction(expression);
        checkArgument(resolvedFunction.isPresent(), "Function has not been analyzed: %s", expression);

        return new Call(
                resolvedFunction.get(),
                expression.getArguments().stream()
                        .map(this::translateExpression)
                        .collect(toImmutableList()));
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
                        .resolveBuiltinFunction("$current_catalog", ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentSchema unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction("$current_schema", ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentPath unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction("$current_path", ImmutableList.of()),
                ImmutableList.of());
    }

    private io.trino.sql.ir.Expression translate(CurrentUser unused)
    {
        return new Call(
                plannerContext.getMetadata()
                        .resolveBuiltinFunction("$current_user", ImmutableList.of()),
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
                .setName("$current_time")
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(CurrentTimestamp node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName("$current_timestamp")
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(LocalTime node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName("$localtime")
                .setArguments(
                        ImmutableList.of(analysis.getType(node)),
                        ImmutableList.of(new Constant(analysis.getType(node), null)))
                .build();
    }

    private io.trino.sql.ir.Expression translate(LocalTimestamp node)
    {
        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName("$localtimestamp")
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
        Type valueType = analysis.getType(node.getValue());
        io.trino.sql.ir.Expression value = translateExpression(node.getValue());

        Type timeZoneType = analysis.getType(node.getTimeZone());
        io.trino.sql.ir.Expression timeZone = translateExpression(node.getTimeZone());

        Call call;
        if (valueType instanceof TimeType type) {
            call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("$at_timezone")
                    .addArgument(createTimeWithTimeZoneType(type.getPrecision()), new io.trino.sql.ir.Cast(value, createTimeWithTimeZoneType(type.getPrecision())))
                    .addArgument(timeZoneType, timeZone)
                    .build();
        }
        else if (valueType instanceof TimeWithTimeZoneType) {
            call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("$at_timezone")
                    .addArgument(valueType, value)
                    .addArgument(timeZoneType, timeZone)
                    .build();
        }
        else if (valueType instanceof TimestampType type) {
            call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("at_timezone")
                    .addArgument(createTimestampWithTimeZoneType(type.getPrecision()), new io.trino.sql.ir.Cast(value, createTimestampWithTimeZoneType(type.getPrecision())))
                    .addArgument(timeZoneType, timeZone)
                    .build();
        }
        else if (valueType instanceof TimestampWithTimeZoneType) {
            call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("at_timezone")
                    .addArgument(valueType, value)
                    .addArgument(timeZoneType, timeZone)
                    .build();
        }
        else {
            throw new IllegalArgumentException("Unexpected type: " + valueType);
        }

        return call;
    }

    private io.trino.sql.ir.Expression translate(Format node)
    {
        List<io.trino.sql.ir.Expression> arguments = node.getArguments().stream()
                .map(this::translateExpression)
                .collect(toImmutableList());
        List<Type> argumentTypes = node.getArguments().stream()
                .map(analysis::getType)
                .collect(toImmutableList());

        Call call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(FormatFunction.NAME)
                .addArgument(VARCHAR, new io.trino.sql.ir.Cast(arguments.get(0), VARCHAR))
                .addArgument(RowType.anonymous(argumentTypes.subList(1, arguments.size())), new io.trino.sql.ir.Row(arguments.subList(1, arguments.size())))
                .build();

        return call;
    }

    private io.trino.sql.ir.Expression translate(TryExpression node)
    {
        Type type = analysis.getType(node);
        io.trino.sql.ir.Expression expression = translateExpression(node.getInnerExpression());

        return BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(TryFunction.NAME)
                .addArgument(new FunctionType(ImmutableList.of(), type), new Lambda(ImmutableList.of(), expression))
                .build();
    }

    private io.trino.sql.ir.Expression translate(LikePredicate node)
    {
        io.trino.sql.ir.Expression value = translateExpression(node.getValue());
        io.trino.sql.ir.Expression pattern = translateExpression(node.getPattern());
        Optional<io.trino.sql.ir.Expression> escape = node.getEscape().map(this::translateExpression);

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

        Call call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName(LIKE_FUNCTION_NAME)
                .addArgument(value.type(), value)
                .addArgument(LIKE_PATTERN, patternCall)
                .build();

        return call;
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

    private io.trino.sql.ir.Expression translate(SubscriptExpression node)
    {
        Type baseType = analysis.getType(node.getBase());
        if (baseType instanceof RowType rowType) {
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

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), path);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.getParametersRow())
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

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), path);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.getParametersRow())
                .add(new Constant(TINYINT, (long) node.getEmptyBehavior().ordinal()))
                .add(node.getEmptyDefault()
                        .map(this::translateExpression)
                        .orElseGet(() -> new Constant(resolvedFunction.get().signature().getReturnType(), null)))
                .add(new Constant(TINYINT, (long) node.getErrorBehavior().ordinal()))
                .add(node.getErrorDefault()
                        .map(this::translateExpression)
                        .orElseGet(() -> new Constant(resolvedFunction.get().signature().getReturnType(), null)));

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

        IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
        io.trino.sql.ir.Expression pathExpression = new Constant(plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), path);

        ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                .add(input)
                .add(pathExpression)
                .add(orderedParameters.getParametersRow())
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
                .map(TypeSignatureTranslator::toTypeSignature)
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
                .map(TypeSignatureTranslator::toTypeSignature)
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
                .map(TypeSignatureTranslator::toTypeSignature)
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

    public static class ParametersRow
    {
        private final io.trino.sql.ir.Expression parametersRow;
        private final List<String> parametersOrder;

        public ParametersRow(io.trino.sql.ir.Expression parametersRow, List<String> parametersOrder)
        {
            this.parametersRow = requireNonNull(parametersRow, "parametersRow is null");
            this.parametersOrder = requireNonNull(parametersOrder, "parametersOrder is null");
        }

        public io.trino.sql.ir.Expression getParametersRow()
        {
            return parametersRow;
        }

        public List<String> getParametersOrder()
        {
            return parametersOrder;
        }
    }
}
