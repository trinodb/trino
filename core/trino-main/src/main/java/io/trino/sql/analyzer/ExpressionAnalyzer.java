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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.FormatFunction;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis.PredicateCoercions;
import io.trino.sql.analyzer.Analysis.Range;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.PatternRecognitionAnalyzer.PatternRecognitionAnalysis;
import io.trino.sql.planner.LiteralInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.StackableAstVisitor;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.type.FunctionType;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.INVALID_NAVIGATION_NESTING;
import static io.trino.spi.StandardErrorCode.INVALID_ORDER_BY;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.INVALID_PATTERN_RECOGNITION_FUNCTION;
import static io.trino.spi.StandardErrorCode.INVALID_PROCESSING_MODE;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_MEASURE;
import static io.trino.spi.StandardErrorCode.MISSING_ORDER_BY;
import static io.trino.spi.StandardErrorCode.MISSING_ROW_PATTERN;
import static io.trino.spi.StandardErrorCode.MISSING_VARIABLE_DEFINITIONS;
import static io.trino.spi.StandardErrorCode.NESTED_AGGREGATION;
import static io.trino.spi.StandardErrorCode.NESTED_WINDOW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.OPERATOR_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.spi.connector.StandardWarningCode.DEPRECATED_FUNCTION;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static io.trino.sql.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.SemanticExceptions.missingAttributeException;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static io.trino.sql.tree.DereferenceExpression.isQualifiedAllFieldsReference;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.PRECEDING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.WindowFrame.Type.GROUPS;
import static io.trino.sql.tree.WindowFrame.Type.RANGE;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static io.trino.type.ArrayParametricType.ARRAY;
import static io.trino.type.DateTimes.extractTimePrecision;
import static io.trino.type.DateTimes.extractTimestampPrecision;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.DateTimes.timeHasTimeZone;
import static io.trino.type.DateTimes.timestampHasTimeZone;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory;
    private final TypeProvider symbolTypes;
    private final boolean isDescribe;

    private final Map<NodeRef<Expression>, ResolvedFunction> resolvedFunctions = new LinkedHashMap<>();
    private final Set<NodeRef<SubqueryExpression>> subqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

    // Coercions needed for window function frame of type RANGE.
    // These are coercions for the sort key, needed for frame bound calculation, identified by frame range offset expression.
    // Frame definition might contain two different offset expressions (for start and end), each requiring different coercion of the sort key.
    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundCalculation = new LinkedHashMap<>();
    // Coercions needed for window function frame of type RANGE.
    // These are coercions for the sort key, needed for comparison of the sort key with precomputed frame bound, identified by frame range offset expression.
    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundComparison = new LinkedHashMap<>();
    // Functions for calculating frame bounds for frame of type RANGE, identified by frame range offset expression.
    private final Map<NodeRef<Expression>, ResolvedFunction> frameBoundCalculations = new LinkedHashMap<>();

    private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, PredicateCoercions> predicateCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();
    private final Set<NodeRef<FunctionCall>> windowFunctions = new LinkedHashSet<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

    // Track referenced fields from source relation node
    private final Multimap<NodeRef<Node>, Field> referencedFields = HashMultimap.create();

    // Record fields prefixed with labels in row pattern recognition context
    private final Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> labelDereferences = new LinkedHashMap<>();
    // Record functions specific to row pattern recognition context
    private final Set<NodeRef<FunctionCall>> patternRecognitionFunctions = new LinkedHashSet<>();
    private final Map<NodeRef<RangeQuantifier>, Range> ranges = new LinkedHashMap<>();
    private final Map<NodeRef<RowPattern>, Set<String>> undefinedLabels = new LinkedHashMap<>();
    private final Map<NodeRef<WindowOperation>, MeasureDefinition> measureDefinitions = new LinkedHashMap<>();
    private final Set<NodeRef<FunctionCall>> patternAggregations = new LinkedHashSet<>();

    private final Session session;
    private final Map<NodeRef<Parameter>, Expression> parameters;
    private final WarningCollector warningCollector;
    private final TypeCoercion typeCoercion;
    private final Function<Expression, Type> getPreanalyzedType;
    private final Function<Node, ResolvedWindow> getResolvedWindow;
    private final List<Field> sourceFields = new ArrayList<>();

    public ExpressionAnalyzer(
            PlannerContext plannerContext,
            AccessControl accessControl,
            StatementAnalyzerFactory statementAnalyzerFactory,
            Analysis analysis,
            Session session,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        this(
                plannerContext,
                accessControl,
                (node, correlationSupport) -> statementAnalyzerFactory.createStatementAnalyzer(
                        analysis,
                        session,
                        warningCollector,
                        correlationSupport),
                session,
                types,
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe(),
                analysis::getType,
                analysis::getWindow);
    }

    ExpressionAnalyzer(
            PlannerContext plannerContext,
            AccessControl accessControl,
            BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory,
            Session session,
            TypeProvider symbolTypes,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe,
            Function<Expression, Type> getPreanalyzedType,
            Function<Node, ResolvedWindow> getResolvedWindow)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        this.getPreanalyzedType = requireNonNull(getPreanalyzedType, "getPreanalyzedType is null");
        this.getResolvedWindow = requireNonNull(getResolvedWindow, "getResolvedWindow is null");
    }

    public Map<NodeRef<Expression>, ResolvedFunction> getResolvedFunctions()
    {
        return unmodifiableMap(resolvedFunctions);
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public Type setExpressionType(Expression expression, Type type)
    {
        requireNonNull(expression, "expression cannot be null");
        requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private Type getExpressionType(Expression expression)
    {
        requireNonNull(expression, "expression cannot be null");

        Type type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, Type> getExpressionCoercions()
    {
        return unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Map<NodeRef<Expression>, Type> getSortKeyCoercionsForFrameBoundCalculation()
    {
        return unmodifiableMap(sortKeyCoercionsForFrameBoundCalculation);
    }

    public Map<NodeRef<Expression>, Type> getSortKeyCoercionsForFrameBoundComparison()
    {
        return unmodifiableMap(sortKeyCoercionsForFrameBoundComparison);
    }

    public Map<NodeRef<Expression>, ResolvedFunction> getFrameBoundCalculations()
    {
        return unmodifiableMap(frameBoundCalculations);
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates()
    {
        return unmodifiableSet(subqueryInPredicates);
    }

    public Map<NodeRef<Expression>, PredicateCoercions> getPredicateCoercions()
    {
        return unmodifiableMap(predicateCoercions);
    }

    public Map<NodeRef<Expression>, ResolvedField> getColumnReferences()
    {
        return unmodifiableMap(columnReferences);
    }

    public Map<NodeRef<Identifier>, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return unmodifiableMap(lambdaArgumentReferences);
    }

    public Type analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope, CorrelationSupport.ALLOWED)));
    }

    public Type analyze(Expression expression, Scope scope, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope, correlationSupport)));
    }

    private Type analyze(Expression expression, Scope scope, Set<String> labels)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.patternRecognition(scope, labels)));
    }

    private Type analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    private void analyzeWindow(ResolvedWindow window, Scope scope, Node originalNode, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        visitor.analyzeWindow(window, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope, correlationSupport)), originalNode);
    }

    public Set<NodeRef<SubqueryExpression>> getSubqueries()
    {
        return unmodifiableSet(subqueries);
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
    {
        return unmodifiableSet(existsSubqueries);
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return unmodifiableSet(quantifiedComparisons);
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions()
    {
        return unmodifiableSet(windowFunctions);
    }

    public Multimap<QualifiedObjectName, String> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    public List<Field> getSourceFields()
    {
        return sourceFields;
    }

    public Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> getLabelDereferences()
    {
        return labelDereferences;
    }

    public Set<NodeRef<FunctionCall>> getPatternRecognitionFunctions()
    {
        return patternRecognitionFunctions;
    }

    public Map<NodeRef<RangeQuantifier>, Range> getRanges()
    {
        return ranges;
    }

    public Map<NodeRef<RowPattern>, Set<String>> getUndefinedLabels()
    {
        return undefinedLabels;
    }

    public Map<NodeRef<WindowOperation>, MeasureDefinition> getMeasureDefinitions()
    {
        return measureDefinitions;
    }

    public Set<NodeRef<FunctionCall>> getPatternAggregations()
    {
        return patternAggregations;
    }

    private class Visitor
            extends StackableAstVisitor<Type, Context>
    {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;
        private final WarningCollector warningCollector;

        public Visitor(Scope baseScope, WarningCollector warningCollector)
        {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        public Type process(Node node, @Nullable StackableAstVisitorContext<Context> context)
        {
            if (node instanceof Expression) {
                // don't double process a node
                Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        protected Type visitRow(Row node, StackableAstVisitorContext<Context> context)
        {
            List<Type> types = node.getItems().stream()
                    .map(child -> process(child, context))
                    .collect(toImmutableList());

            Type type = RowType.anonymous(types);
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, StackableAstVisitorContext<Context> context)
        {
            switch (node.getFunction()) {
                case DATE:
                    checkArgument(node.getPrecision() == null);
                    return setExpressionType(node, DATE);
                case TIME:
                    if (node.getPrecision() != null) {
                        return setExpressionType(node, createTimeWithTimeZoneType(node.getPrecision()));
                    }
                    return setExpressionType(node, TIME_WITH_TIME_ZONE);
                case LOCALTIME:
                    if (node.getPrecision() != null) {
                        return setExpressionType(node, createTimeType(node.getPrecision()));
                    }
                    return setExpressionType(node, TIME);
                case TIMESTAMP:
                    return setExpressionType(node, createTimestampWithTimeZoneType(firstNonNull(node.getPrecision(), TimestampWithTimeZoneType.DEFAULT_PRECISION)));
                case LOCALTIMESTAMP:
                    if (node.getPrecision() != null) {
                        return setExpressionType(node, createTimestampType(node.getPrecision()));
                    }
                    return setExpressionType(node, TIMESTAMP_MILLIS);
            }
            throw semanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getFunction().getName());
        }

        @Override
        protected Type visitSymbolReference(SymbolReference node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                Optional<ResolvedField> resolvedField = context.getContext().getScope().tryResolveField(node, QualifiedName.of(node.getName()));
                if (resolvedField.isPresent() && context.getContext().getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
                    return setExpressionType(node, resolvedField.get().getType());
                }
            }
            Type type = symbolTypes.get(Symbol.from(node));
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context)
        {
            ResolvedField resolvedField = context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context)
        {
            if (!resolvedField.isLocal() && context.getContext().getCorrelationSupport() != CorrelationSupport.ALLOWED) {
                throw semanticException(NOT_SUPPORTED, node, "Reference to column '%s' from outer scope not allowed in this context", node);
            }

            FieldId fieldId = FieldId.from(resolvedField);
            Field field = resolvedField.getField();
            if (context.getContext().isInLambda()) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getContext().getFieldToLambdaArgumentDeclaration().get(fieldId);
                if (lambdaArgumentDeclaration != null) {
                    // Lambda argument reference is not a column reference
                    lambdaArgumentReferences.put(NodeRef.of((Identifier) node), lambdaArgumentDeclaration);
                    return setExpressionType(node, field.getType());
                }
            }

            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            sourceFields.add(field);

            fieldId.getRelationId()
                    .getSourceNode()
                    .ifPresent(source -> referencedFields.put(NodeRef.of(source), field));

            ResolvedField previous = columnReferences.put(NodeRef.of(node), resolvedField);
            checkState(previous == null, "%s already known to refer to %s", node, previous);

            return setExpressionType(node, field.getType());
        }

        @Override
        protected Type visitDereferenceExpression(DereferenceExpression node, StackableAstVisitorContext<Context> context)
        {
            if (isQualifiedAllFieldsReference(node)) {
                throw semanticException(NOT_SUPPORTED, node, "<identifier>.* not allowed in this context");
            }

            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                // In the context of row pattern matching, fields are optionally prefixed with labels. Labels are irrelevant during type analysis.
                if (context.getContext().isPatternRecognition()) {
                    String label = label(qualifiedName.getOriginalParts().get(0));
                    if (context.getContext().getLabels().contains(label)) {
                        // In the context of row pattern matching, the name of row pattern input table cannot be used to qualify column names.
                        // (it can only be accessed in PARTITION BY and ORDER BY clauses of MATCH_RECOGNIZE). Consequentially, if a dereference
                        // expression starts with a label, the next part must be a column.
                        // Only strict column references can be prefixed by label. Labeled references to row fields are not supported.
                        QualifiedName unlabeledName = QualifiedName.of(qualifiedName.getOriginalParts().subList(1, qualifiedName.getOriginalParts().size()));
                        if (qualifiedName.getOriginalParts().size() > 2) {
                            throw semanticException(COLUMN_NOT_FOUND, node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        Identifier unlabeled = qualifiedName.getOriginalParts().get(1);
                        Optional<ResolvedField> resolvedField = context.getContext().getScope().tryResolveField(node, unlabeledName);
                        if (resolvedField.isEmpty()) {
                            throw semanticException(COLUMN_NOT_FOUND, node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        // Correlation is not allowed in pattern recognition context. Visitor's context for pattern recognition has CorrelationSupport.DISALLOWED,
                        // and so the following call should fail if the field is from outer scope.
                        Type type = process(unlabeled, new StackableAstVisitorContext<>(context.getContext().notExpectingLabels()));
                        labelDereferences.put(NodeRef.of(node), new LabelPrefixedReference(label, unlabeled));
                        return setExpressionType(node, type);
                    }
                    // In the context of row pattern matching, qualified column references are not allowed.
                    throw missingAttributeException(node, qualifiedName);
                }

                Scope scope = context.getContext().getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                if (!scope.isColumnReference(qualifiedName)) {
                    throw missingAttributeException(node, qualifiedName);
                }
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType)) {
                throw semanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            RowType rowType = (RowType) baseType;
            Identifier field = node.getField().orElseThrow();
            String fieldName = field.getValue();

            boolean foundFieldName = false;
            Type rowFieldType = null;
            for (RowType.Field rowField : rowType.getFields()) {
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    if (foundFieldName) {
                        throw semanticException(AMBIGUOUS_NAME, field, "Ambiguous row field reference: " + fieldName);
                    }
                    foundFieldName = true;
                    rowFieldType = rowField.getType();
                }
            }

            if (rowFieldType == null) {
                throw missingAttributeException(node, qualifiedName);
            }

            return setExpressionType(node, rowFieldType);
        }

        @Override
        protected Type visitNotExpression(NotExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitLogicalExpression(LogicalExpression node, StackableAstVisitorContext<Context> context)
        {
            for (Expression term : node.getTerms()) {
                coerceType(context, term, BOOLEAN, "Logical expression term");
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            OperatorType operatorType;
            switch (node.getOperator()) {
                case EQUAL:
                case NOT_EQUAL:
                    operatorType = OperatorType.EQUAL;
                    break;
                case LESS_THAN:
                case GREATER_THAN:
                    operatorType = OperatorType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    operatorType = OperatorType.LESS_THAN_OR_EQUAL;
                    break;
                case IS_DISTINCT_FROM:
                    operatorType = OperatorType.IS_DISTINCT_FROM;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported comparison operator: " + node.getOperator());
            }
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, StackableAstVisitorContext<Context> context)
        {
            Type firstType = process(node.getFirst(), context);
            Type secondType = process(node.getSecond(), context);

            if (typeCoercion.getCommonSuperType(firstType, secondType).isEmpty()) {
                throw semanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            return setExpressionType(node, firstType);
        }

        @Override
        protected Type visitIfExpression(IfExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same: %s vs %s", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
            }

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceCaseOperandToToSingleType(node, context);

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private void coerceCaseOperandToToSingleType(SimpleCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            Type operandType = process(node.getOperand(), context);

            List<WhenClause> whenClauses = node.getWhenClauses();
            List<Type> whenOperandTypes = new ArrayList<>(whenClauses.size());

            Type commonType = operandType;
            for (WhenClause whenClause : whenClauses) {
                Expression whenOperand = whenClause.getOperand();
                Type whenOperandType = process(whenOperand, context);
                whenOperandTypes.add(whenOperandType);

                Optional<Type> operandCommonType = typeCoercion.getCommonSuperType(commonType, whenOperandType);

                if (operandCommonType.isEmpty()) {
                    throw semanticException(TYPE_MISMATCH, whenOperand, "CASE operand type does not match WHEN clause operand type: %s vs %s", operandType, whenOperandType);
                }

                commonType = operandCommonType.get();
            }

            if (commonType != operandType) {
                addOrReplaceExpressionCoercion(node.getOperand(), operandType, commonType);
            }

            for (int i = 0; i < whenOperandTypes.size(); i++) {
                Type whenOperandType = whenOperandTypes.get(i);
                if (!whenOperandType.equals(commonType)) {
                    Expression whenOperand = whenClauses.get(i).getOperand();
                    addOrReplaceExpressionCoercion(whenOperand, whenOperandType, commonType);
                }
            }
        }

        private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses, Optional<Expression> defaultValue)
        {
            List<Expression> resultExpressions = new ArrayList<>();
            for (WhenClause whenClause : whenClauses) {
                resultExpressions.add(whenClause.getResult());
            }
            defaultValue.ifPresent(resultExpressions::add);
            return resultExpressions;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, StackableAstVisitorContext<Context> context)
        {
            Type type = coerceToSingleType(context, "All COALESCE operands must be the same type: %s", node.getOperands());

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, StackableAstVisitorContext<Context> context)
        {
            switch (node.getSign()) {
                case PLUS:
                    Type type = process(node.getValue(), context);

                    if (!type.equals(DOUBLE) && !type.equals(REAL) && !type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw semanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    return setExpressionType(node, type);
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, StackableAstVisitorContext<Context> context)
        {
            Type valueType = process(node.getValue(), context);
            if (!(valueType instanceof CharType) && !(valueType instanceof VarcharType)) {
                coerceType(context, node.getValue(), VARCHAR, "Left side of LIKE expression");
            }

            Type patternType = process(node.getPattern(), context);
            if (!(patternType instanceof VarcharType)) {
                // TODO can pattern be of char type?
                coerceType(context, node.getPattern(), VARCHAR, "Pattern for LIKE expression");
            }
            if (node.getEscape().isPresent()) {
                Expression escape = node.getEscape().get();
                Type escapeType = process(escape, context);
                if (!(escapeType instanceof VarcharType)) {
                    // TODO can escape be of char type?
                    coerceType(context, escape, VARCHAR, "Escape for LIKE expression");
                }
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitSubscriptExpression(SubscriptExpression node, StackableAstVisitorContext<Context> context)
        {
            Type baseType = process(node.getBase(), context);
            // Subscript on Row hasn't got a dedicated operator. Its Type is resolved by hand.
            if (baseType instanceof RowType) {
                if (!(node.getIndex() instanceof LongLiteral)) {
                    throw semanticException(EXPRESSION_NOT_CONSTANT, node.getIndex(), "Subscript expression on ROW requires a constant index");
                }
                Type indexType = process(node.getIndex(), context);
                if (!indexType.equals(INTEGER)) {
                    throw semanticException(TYPE_MISMATCH, node.getIndex(), "Subscript expression on ROW requires integer index, found %s", indexType);
                }
                int indexValue = toIntExact(((LongLiteral) node.getIndex()).getValue());
                if (indexValue <= 0) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, node.getIndex(), "Invalid subscript index: %s. ROW indices start at 1", indexValue);
                }
                List<Type> rowTypes = baseType.getTypeParameters();
                if (indexValue > rowTypes.size()) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, node.getIndex(), "Subscript index out of bounds: %s, max value is %s", indexValue, rowTypes.size());
                }
                return setExpressionType(node, rowTypes.get(indexValue - 1));
            }

            // Subscript on Array or Map uses an operator to resolve Type.
            return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
        }

        @Override
        protected Type visitArrayConstructor(ArrayConstructor node, StackableAstVisitorContext<Context> context)
        {
            Type type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s", node.getValues());
            Type arrayType = plannerContext.getTypeManager().getParameterizedType(ARRAY.getName(), ImmutableList.of(TypeSignatureParameter.typeParameter(type.getTypeSignature())));
            return setExpressionType(node, arrayType);
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, StackableAstVisitorContext<Context> context)
        {
            VarcharType type = VarcharType.createVarcharType(node.length());
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitCharLiteral(CharLiteral node, StackableAstVisitorContext<Context> context)
        {
            CharType type = CharType.createCharType(node.length());
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitBinaryLiteral(BinaryLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARBINARY);
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, DOUBLE);
        }

        @Override
        protected Type visitDecimalLiteral(DecimalLiteral node, StackableAstVisitorContext<Context> context)
        {
            DecimalParseResult parseResult;
            try {
                parseResult = Decimals.parse(node.getValue());
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid decimal literal", node.getValue());
            }
            return setExpressionType(node, parseResult.getType());
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitGenericLiteral(GenericLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                type = plannerContext.getTypeManager().fromSqlType(node.getType());
            }
            catch (TypeNotFoundException e) {
                throw semanticException(TYPE_NOT_FOUND, node, "Unknown type: %s", node.getType());
            }

            if (!JSON.equals(type)) {
                try {
                    plannerContext.getMetadata().getCoercion(session, VARCHAR, type);
                }
                catch (IllegalArgumentException e) {
                    throw semanticException(INVALID_LITERAL, node, "No literal form for type %s", type);
                }
            }
            try {
                LiteralInterpreter.evaluate(plannerContext, session, ImmutableMap.of(NodeRef.of(node), type), node);
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid %s literal", node.getValue(), type.getDisplayName());
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                int precision = extractTimePrecision(node.getValue());

                if (timeHasTimeZone(node.getValue())) {
                    type = createTimeWithTimeZoneType(precision);
                    parseTimeWithTimeZone(precision, node.getValue());
                }
                else {
                    type = createTimeType(precision);
                    parseTime(node.getValue());
                }
            }
            catch (TrinoException e) {
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            catch (IllegalArgumentException e) {
                throw semanticException(INVALID_LITERAL, node, "'%s' is not a valid time literal", node.getValue());
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                if (timestampHasTimeZone(node.getValue())) {
                    int precision = extractTimestampPrecision(node.getValue());
                    type = createTimestampWithTimeZoneType(precision);
                    parseTimestampWithTimeZone(precision, node.getValue());
                }
                else {
                    int precision = extractTimestampPrecision(node.getValue());
                    type = createTimestampType(precision);
                    parseTimestamp(precision, node.getValue());
                }
            }
            catch (TrinoException e) {
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            catch (Exception e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid timestamp literal", node.getValue());
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH;
            }
            else {
                type = INTERVAL_DAY_TIME;
            }
            try {
                LiteralInterpreter.evaluate(plannerContext, session, ImmutableMap.of(NodeRef.of(node), type), node);
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid interval literal", node.getValue());
            }
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, UNKNOWN);
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, StackableAstVisitorContext<Context> context)
        {
            boolean isRowPatternCount = context.getContext().isPatternRecognition() &&
                    plannerContext.getMetadata().isAggregationFunction(node.getName()) &&
                    node.getName().getSuffix().equalsIgnoreCase("count");
            // argument of the form `label.*` is only allowed for row pattern count function
            node.getArguments().stream()
                    .filter(DereferenceExpression::isQualifiedAllFieldsReference)
                    .findAny()
                    .ifPresent(allRowsReference -> {
                        if (!isRowPatternCount || node.getArguments().size() > 1) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, allRowsReference, "label.* syntax is only supported as the only argument of row pattern count function");
                        }
                    });
            if (context.getContext().isPatternRecognition() && isPatternRecognitionFunction(node)) {
                return analyzePatternRecognitionFunction(node, context);
            }
            if (context.getContext().isPatternRecognition() && plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                analyzePatternAggregation(node);
                patternAggregations.add(NodeRef.of(node));
            }
            if (node.getProcessingMode().isPresent()) {
                ProcessingMode processingMode = node.getProcessingMode().get();
                if (!context.getContext().isPatternRecognition()) {
                    throw semanticException(INVALID_PROCESSING_MODE, processingMode, "%s semantics is not supported out of pattern recognition context", processingMode.getMode());
                }
                if (!plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                    throw semanticException(INVALID_PROCESSING_MODE, processingMode, "%s semantics is supported only for FIRST(), LAST() and aggregation functions. Actual: %s", processingMode.getMode(), node.getName());
                }
            }

            if (node.getWindow().isPresent()) {
                ResolvedWindow window = getResolvedWindow.apply(node);
                checkState(window != null, "no resolved window for: " + node);

                analyzeWindow(window, context, (Node) node.getWindow().get());
                windowFunctions.add(NodeRef.of(node));
            }
            else {
                if (node.isDistinct() && !plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "DISTINCT is not supported for non-aggregation functions");
                }
            }

            if (node.getFilter().isPresent()) {
                Expression expression = node.getFilter().get();
                process(expression, context);
            }

            List<TypeSignatureProvider> argumentTypes = getCallArgumentTypes(node.getArguments(), context);

            if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                // Due to fact that the LISTAGG function is transformed out of pragmatic reasons
                // in a synthetic function call, the type expression of this function call is evaluated
                // explicitly here in order to make sure that it is a varchar.
                List<Expression> arguments = node.getArguments();
                Expression expression = arguments.get(0);
                Type expressionType = process(expression, context);
                if (!(expressionType instanceof VarcharType)) {
                    throw semanticException(TYPE_MISMATCH, node, format("Expected expression of varchar, but '%s' has %s type", expression, expressionType.getDisplayName()));
                }
            }

            // must run after arguments are processed and labels are recorded
            if (context.getContext().isPatternRecognition() && plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                validateAggregationLabelConsistency(node);
            }

            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveFunction(session, node.getName(), argumentTypes);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    // If analysis of any of the argument types (which is done lazily to deal with lambda
                    // expressions) fails, we want to report the original reason for the failure
                    throw e;
                }

                // otherwise, it must have failed due to a missing function or other reason, so we report an error at the
                // current location

                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }

            if (function.getSignature().getName().equalsIgnoreCase(ARRAY_CONSTRUCTOR)) {
                // After optimization, array constructor is rewritten to a function call.
                // For historic reasons array constructor is allowed to have 254 arguments
                if (node.getArguments().size() > 254) {
                    throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for array constructor", function.getSignature().getName());
                }
            }
            else if (node.getArguments().size() > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for function call %s()", function.getSignature().getName());
            }

            if (node.getOrderBy().isPresent()) {
                for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
                    Type sortKeyType = process(sortItem.getSortKey(), context);
                    if (!sortKeyType.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, node, "ORDER BY can only be applied to orderable types (actual: %s)", sortKeyType.getDisplayName());
                    }
                }
            }

            BoundSignature signature = function.getSignature();
            for (int i = 0; i < argumentTypes.size(); i++) {
                Expression expression = node.getArguments().get(i);
                Type expectedType = signature.getArgumentTypes().get(i);
                requireNonNull(expectedType, format("Type '%s' not found", signature.getArgumentTypes().get(i)));
                if (node.isDistinct() && !expectedType.isComparable()) {
                    throw semanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
                }
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, new StackableAstVisitorContext<>(context.getContext().expectingLambda(expectedFunctionType.getArgumentTypes())));
                }
                else {
                    Type actualType = plannerContext.getTypeManager().getType(argumentTypes.get(i).getTypeSignature());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            accessControl.checkCanExecuteFunction(SecurityContext.of(session), node.getName().toString());

            resolvedFunctions.put(NodeRef.of(node), function);

            FunctionMetadata functionMetadata = plannerContext.getMetadata().getFunctionMetadata(function);
            if (functionMetadata.isDeprecated()) {
                warningCollector.add(new TrinoWarning(DEPRECATED_FUNCTION,
                        format("Use of deprecated function: %s: %s",
                                functionMetadata.getSignature().getName(),
                                functionMetadata.getDescription())));
            }

            Type type = signature.getReturnType();
            return setExpressionType(node, type);
        }

        private void analyzeWindow(ResolvedWindow window, StackableAstVisitorContext<Context> context, Node originalNode)
        {
            // check no nested window functions
            ImmutableList.Builder<Node> childNodes = ImmutableList.builder();
            if (!window.isPartitionByInherited()) {
                childNodes.addAll(window.getPartitionBy());
            }
            if (!window.isOrderByInherited()) {
                window.getOrderBy().ifPresent(orderBy -> childNodes.addAll(orderBy.getSortItems()));
            }
            if (!window.isFrameInherited()) {
                window.getFrame().ifPresent(childNodes::add);
            }
            List<Expression> nestedWindowExpressions = extractWindowExpressions(childNodes.build());
            if (!nestedWindowExpressions.isEmpty()) {
                throw semanticException(NESTED_WINDOW, nestedWindowExpressions.get(0), "Cannot nest window functions or row pattern measures inside window specification");
            }

            if (!window.isPartitionByInherited()) {
                for (Expression expression : window.getPartitionBy()) {
                    process(expression, context);
                    Type type = getExpressionType(expression);
                    if (!type.isComparable()) {
                        throw semanticException(TYPE_MISMATCH, expression, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }
            }

            if (!window.isOrderByInherited()) {
                for (SortItem sortItem : getSortItemsFromOrderBy(window.getOrderBy())) {
                    process(sortItem.getSortKey(), context);
                    Type type = getExpressionType(sortItem.getSortKey());
                    if (!type.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, sortItem, "%s is not orderable, and therefore cannot be used in window function ORDER BY", type);
                    }
                }
            }

            if (window.getFrame().isPresent() && !window.isFrameInherited()) {
                WindowFrame frame = window.getFrame().get();

                if (frame.getPattern().isPresent()) {
                    if (frame.getVariableDefinitions().isEmpty()) {
                        throw semanticException(MISSING_VARIABLE_DEFINITIONS, frame, "Pattern recognition requires DEFINE clause");
                    }
                    if (frame.getType() != ROWS) {
                        throw semanticException(INVALID_WINDOW_FRAME, frame, "Pattern recognition requires ROWS frame type");
                    }
                    if (frame.getStart().getType() != CURRENT_ROW || frame.getEnd().isEmpty()) {
                        throw semanticException(INVALID_WINDOW_FRAME, frame, "Pattern recognition requires frame specified as BETWEEN CURRENT ROW AND ...");
                    }
                    PatternRecognitionAnalysis analysis = PatternRecognitionAnalyzer.analyze(
                            frame.getSubsets(),
                            frame.getVariableDefinitions(),
                            frame.getMeasures(),
                            frame.getPattern().get(),
                            frame.getAfterMatchSkipTo());

                    ranges.putAll(analysis.getRanges());
                    undefinedLabels.put(NodeRef.of(frame.getPattern().get()), analysis.getUndefinedLabels());

                    PatternRecognitionAnalyzer.validateNoPatternAnchors(frame.getPattern().get());

                    // analyze expressions in MEASURES and DEFINE (with set of all labels passed as context)
                    for (VariableDefinition variableDefinition : frame.getVariableDefinitions()) {
                        Expression expression = variableDefinition.getExpression();
                        Type type = process(expression, new StackableAstVisitorContext<>(context.getContext().patternRecognition(analysis.getAllLabels())));
                        if (!type.equals(BOOLEAN)) {
                            throw semanticException(TYPE_MISMATCH, expression, "Expression defining a label must be boolean (actual type: %s)", type);
                        }
                    }
                    for (MeasureDefinition measureDefinition : frame.getMeasures()) {
                        Expression expression = measureDefinition.getExpression();
                        process(expression, new StackableAstVisitorContext<>(context.getContext().patternRecognition(analysis.getAllLabels())));
                    }

                    // validate pattern recognition expressions: MATCH_NUMBER() is not allowed in window
                    // this must run after the expressions in MEASURES and DEFINE are analyzed, and the patternRecognitionFunctions are recorded
                    PatternRecognitionAnalyzer.validateNoMatchNumber(frame.getMeasures(), frame.getVariableDefinitions(), patternRecognitionFunctions);

                    // TODO prohibited nesting: pattern recognition in frame end expression(?)
                }
                else {
                    if (!frame.getMeasures().isEmpty()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame, "Row pattern measures require PATTERN clause");
                    }
                    if (frame.getAfterMatchSkipTo().isPresent()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame.getAfterMatchSkipTo().get(), "AFTER MATCH SKIP clause requires PATTERN clause");
                    }
                    if (frame.getPatternSearchMode().isPresent()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame.getPatternSearchMode().get(), "%s modifier requires PATTERN clause", frame.getPatternSearchMode().get().getMode().name());
                    }
                    if (!frame.getSubsets().isEmpty()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame.getSubsets().get(0), "Union variable definitions require PATTERN clause");
                    }
                    if (!frame.getVariableDefinitions().isEmpty()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame.getVariableDefinitions().get(0), "Primary pattern variable definitions require PATTERN clause");
                    }
                }

                // validate frame start and end types
                FrameBound.Type startType = frame.getStart().getType();
                FrameBound.Type endType = frame.getEnd().orElse(new FrameBound(CURRENT_ROW)).getType();
                if (startType == UNBOUNDED_FOLLOWING) {
                    throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame start cannot be UNBOUNDED FOLLOWING");
                }
                if (endType == UNBOUNDED_PRECEDING) {
                    throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame end cannot be UNBOUNDED PRECEDING");
                }
                if ((startType == CURRENT_ROW) && (endType == PRECEDING)) {
                    throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from CURRENT ROW cannot end with PRECEDING");
                }
                if ((startType == FOLLOWING) && (endType == PRECEDING)) {
                    throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with PRECEDING");
                }
                if ((startType == FOLLOWING) && (endType == CURRENT_ROW)) {
                    throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with CURRENT ROW");
                }

                // analyze frame offset values
                if (frame.getType() == ROWS) {
                    if (frame.getStart().getValue().isPresent()) {
                        Expression startValue = frame.getStart().getValue().get();
                        Type type = process(startValue, context);
                        if (!isExactNumericWithScaleZero(type)) {
                            throw semanticException(TYPE_MISMATCH, startValue, "Window frame ROWS start value type must be exact numeric type with scale 0 (actual %s)", type);
                        }
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        Expression endValue = frame.getEnd().get().getValue().get();
                        Type type = process(endValue, context);
                        if (!isExactNumericWithScaleZero(type)) {
                            throw semanticException(TYPE_MISMATCH, endValue, "Window frame ROWS end value type must be exact numeric type with scale 0 (actual %s)", type);
                        }
                    }
                }
                else if (frame.getType() == RANGE) {
                    if (frame.getStart().getValue().isPresent()) {
                        Expression startValue = frame.getStart().getValue().get();
                        analyzeFrameRangeOffset(startValue, frame.getStart().getType(), context, window, originalNode);
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        Expression endValue = frame.getEnd().get().getValue().get();
                        analyzeFrameRangeOffset(endValue, frame.getEnd().get().getType(), context, window, originalNode);
                    }
                }
                else if (frame.getType() == GROUPS) {
                    if (frame.getStart().getValue().isPresent()) {
                        if (window.getOrderBy().isEmpty()) {
                            throw semanticException(MISSING_ORDER_BY, originalNode, "Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY");
                        }
                        Expression startValue = frame.getStart().getValue().get();
                        Type type = process(startValue, context);
                        if (!isExactNumericWithScaleZero(type)) {
                            throw semanticException(TYPE_MISMATCH, startValue, "Window frame GROUPS start value type must be exact numeric type with scale 0 (actual %s)", type);
                        }
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        if (window.getOrderBy().isEmpty()) {
                            throw semanticException(MISSING_ORDER_BY, originalNode, "Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY");
                        }
                        Expression endValue = frame.getEnd().get().getValue().get();
                        Type type = process(endValue, context);
                        if (!isExactNumericWithScaleZero(type)) {
                            throw semanticException(TYPE_MISMATCH, endValue, "Window frame GROUPS end value type must be exact numeric type with scale 0 (actual %s)", type);
                        }
                    }
                }
                else {
                    throw semanticException(NOT_SUPPORTED, frame, "Unsupported frame type: " + frame.getType());
                }
            }
        }

        private void analyzeFrameRangeOffset(Expression offsetValue, FrameBound.Type boundType, StackableAstVisitorContext<Context> context, ResolvedWindow window, Node originalNode)
        {
            if (window.getOrderBy().isEmpty()) {
                throw semanticException(MISSING_ORDER_BY, originalNode, "Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY");
            }
            OrderBy orderBy = window.getOrderBy().get();
            if (orderBy.getSortItems().size() != 1) {
                throw semanticException(INVALID_ORDER_BY, orderBy, "Window frame of type RANGE PRECEDING or FOLLOWING requires single sort item in ORDER BY (actual: %s)", orderBy.getSortItems().size());
            }
            Expression sortKey = Iterables.getOnlyElement(orderBy.getSortItems()).getSortKey();
            Type sortKeyType;
            if (window.isOrderByInherited()) {
                sortKeyType = getPreanalyzedType.apply(sortKey);
            }
            else {
                sortKeyType = getExpressionType(sortKey);
            }
            if (!isNumericType(sortKeyType) && !isDateTimeType(sortKeyType)) {
                throw semanticException(TYPE_MISMATCH, sortKey, "Window frame of type RANGE PRECEDING or FOLLOWING requires that sort item type be numeric, datetime or interval (actual: %s)", sortKeyType);
            }

            Type offsetValueType = process(offsetValue, context);

            if (isNumericType(sortKeyType)) {
                if (!isNumericType(offsetValueType)) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
            }
            else { // isDateTimeType(sortKeyType)
                if (offsetValueType != INTERVAL_DAY_TIME && offsetValueType != INTERVAL_YEAR_MONTH) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
            }

            // resolve function to calculate frame boundary value (add / subtract offset from sortKey)
            Ordering ordering = Iterables.getOnlyElement(orderBy.getSortItems()).getOrdering();
            OperatorType operatorType;
            ResolvedFunction function;
            if ((boundType == PRECEDING && ordering == ASCENDING) || (boundType == FOLLOWING && ordering == DESCENDING)) {
                operatorType = SUBTRACT;
            }
            else {
                operatorType = ADD;
            }
            try {
                function = plannerContext.getMetadata().resolveOperator(session, operatorType, ImmutableList.of(sortKeyType, offsetValueType));
            }
            catch (TrinoException e) {
                ErrorCode errorCode = e.getErrorCode();
                if (errorCode.equals(OPERATOR_NOT_FOUND.toErrorCode())) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
                throw e;
            }
            BoundSignature signature = function.getSignature();
            Type expectedSortKeyType = signature.getArgumentTypes().get(0);
            if (!expectedSortKeyType.equals(sortKeyType)) {
                if (!typeCoercion.canCoerce(sortKeyType, expectedSortKeyType)) {
                    throw semanticException(TYPE_MISMATCH, sortKey, "Sort key must evaluate to a %s (actual: %s)", expectedSortKeyType, sortKeyType);
                }
                sortKeyCoercionsForFrameBoundCalculation.put(NodeRef.of(offsetValue), expectedSortKeyType);
            }
            Type expectedOffsetValueType = signature.getArgumentTypes().get(1);
            if (!expectedOffsetValueType.equals(offsetValueType)) {
                coerceType(offsetValue, offsetValueType, expectedOffsetValueType, format("Function %s argument 1", function));
            }
            Type expectedFunctionResultType = signature.getReturnType();
            if (!expectedFunctionResultType.equals(sortKeyType)) {
                if (!typeCoercion.canCoerce(sortKeyType, expectedFunctionResultType)) {
                    throw semanticException(TYPE_MISMATCH, sortKey, "Sort key must evaluate to a %s (actual: %s)", expectedFunctionResultType, sortKeyType);
                }
                sortKeyCoercionsForFrameBoundComparison.put(NodeRef.of(offsetValue), expectedFunctionResultType);
            }

            frameBoundCalculations.put(NodeRef.of(offsetValue), function);
        }

        @Override
        protected Type visitWindowOperation(WindowOperation node, StackableAstVisitorContext<Context> context)
        {
            ResolvedWindow window = getResolvedWindow.apply(node);
            checkState(window != null, "no resolved window for: " + node);

            if (window.getFrame().isEmpty()) {
                throw semanticException(INVALID_WINDOW_MEASURE, node, "Measure %s is not defined in the corresponding window", node.getName().getValue());
            }
            CanonicalizationAware<Identifier> canonicalName = canonicalizationAwareKey(node.getName());
            List<MeasureDefinition> matchingMeasures = window.getFrame().get().getMeasures().stream()
                    .filter(measureDefinition -> canonicalizationAwareKey(measureDefinition.getName()).equals(canonicalName))
                    .collect(toImmutableList());
            if (matchingMeasures.isEmpty()) {
                throw semanticException(INVALID_WINDOW_MEASURE, node, "Measure %s is not defined in the corresponding window", node.getName().getValue());
            }
            if (matchingMeasures.size() > 1) {
                throw semanticException(AMBIGUOUS_NAME, node, "Measure %s is defined more than once", node.getName().getValue());
            }
            MeasureDefinition matchingMeasure = getOnlyElement(matchingMeasures);
            measureDefinitions.put(NodeRef.of(node), matchingMeasure);

            analyzeWindow(window, context, (Node) node.getWindow());

            Expression expression = matchingMeasure.getExpression();
            Type type = window.isFrameInherited() ? getPreanalyzedType.apply(expression) : getExpressionType(expression);

            return setExpressionType(node, type);
        }

        public List<TypeSignatureProvider> getCallArgumentTypes(List<Expression> arguments, StackableAstVisitorContext<Context> context)
        {
            ImmutableList.Builder<TypeSignatureProvider> argumentTypesBuilder = ImmutableList.builder();
            for (Expression argument : arguments) {
                if (argument instanceof LambdaExpression || argument instanceof BindExpression) {
                    argumentTypesBuilder.add(new TypeSignatureProvider(
                            types -> {
                                ExpressionAnalyzer innerExpressionAnalyzer = new ExpressionAnalyzer(
                                        plannerContext,
                                        accessControl,
                                        statementAnalyzerFactory,
                                        session,
                                        symbolTypes,
                                        parameters,
                                        warningCollector,
                                        isDescribe,
                                        getPreanalyzedType,
                                        getResolvedWindow);
                                if (context.getContext().isInLambda()) {
                                    for (LambdaArgumentDeclaration lambdaArgument : context.getContext().getFieldToLambdaArgumentDeclaration().values()) {
                                        innerExpressionAnalyzer.setExpressionType(lambdaArgument, getExpressionType(lambdaArgument));
                                    }
                                }
                                return innerExpressionAnalyzer.analyze(argument, baseScope, context.getContext().expectingLambda(types)).getTypeSignature();
                            }));
                }
                else {
                    if (isQualifiedAllFieldsReference(argument)) {
                        // to resolve `count(label.*)` correctly, we should skip the argument, like for `count(*)`
                        // process the argument but do not include it in the list
                        DereferenceExpression allRowsDereference = (DereferenceExpression) argument;
                        String label = label((Identifier) allRowsDereference.getBase());
                        if (!context.getContext().getLabels().contains(label)) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, allRowsDereference.getBase(), "%s is not a primary pattern variable or subset name", label);
                        }
                        labelDereferences.put(NodeRef.of(allRowsDereference), new LabelPrefixedReference(label));
                    }
                    else {
                        argumentTypesBuilder.add(new TypeSignatureProvider(process(argument, context).getTypeSignature()));
                    }
                }
            }

            return argumentTypesBuilder.build();
        }

        private Type analyzePatternRecognitionFunction(FunctionCall node, StackableAstVisitorContext<Context> context)
        {
            if (node.getWindow().isPresent()) {
                throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, node, "Cannot use OVER with %s pattern recognition function", node.getName());
            }
            if (node.getFilter().isPresent()) {
                throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, node, "Cannot use FILTER with %s pattern recognition function", node.getName());
            }
            if (node.getOrderBy().isPresent()) {
                throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, node, "Cannot use ORDER BY with %s pattern recognition function", node.getName());
            }
            if (node.isDistinct()) {
                throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, node, "Cannot use DISTINCT with %s pattern recognition function", node.getName());
            }
            String name = node.getName().getSuffix();
            if (node.getProcessingMode().isPresent()) {
                ProcessingMode processingMode = node.getProcessingMode().get();
                if (!name.equalsIgnoreCase("FIRST") && !name.equalsIgnoreCase("LAST")) {
                    throw semanticException(INVALID_PROCESSING_MODE, processingMode, "%s semantics is not supported with %s pattern recognition function", processingMode.getMode(), node.getName());
                }
            }

            patternRecognitionFunctions.add(NodeRef.of(node));

            switch (name.toUpperCase(ENGLISH)) {
                case "FIRST":
                case "LAST":
                case "PREV":
                case "NEXT":
                    if (node.getArguments().size() != 1 && node.getArguments().size() != 2) {
                        throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s pattern recognition function requires 1 or 2 arguments", node.getName());
                    }
                    Type resultType = process(node.getArguments().get(0), context);
                    if (node.getArguments().size() == 2) {
                        process(node.getArguments().get(1), context);
                        // TODO the offset argument must be effectively constant, not necessarily a number. This could be extended with the use of ConstantAnalyzer.
                        if (!(node.getArguments().get(1) instanceof LongLiteral)) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s pattern recognition navigation function requires a number as the second argument", node.getName());
                        }
                        long offset = ((LongLiteral) node.getArguments().get(1)).getValue();
                        if (offset < 0) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "%s pattern recognition navigation function requires a non-negative number as the second argument (actual: %s)", node.getName(), offset);
                        }
                        if (offset > Integer.MAX_VALUE) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "The second argument of %s pattern recognition navigation function must not exceed %s (actual: %s)", node.getName(), Integer.MAX_VALUE, offset);
                        }
                    }
                    validateNavigationNesting(node);
                    checkNoNestedAggregations(node);

                    // must run after the argument is processed and labels in the argument are recorded
                    validateNavigationLabelConsistency(node);
                    return setExpressionType(node, resultType);
                case "MATCH_NUMBER":
                    if (!node.getArguments().isEmpty()) {
                        throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "MATCH_NUMBER pattern recognition function takes no arguments");
                    }
                    return setExpressionType(node, BIGINT);
                case "CLASSIFIER":
                    if (node.getArguments().size() > 1) {
                        throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "CLASSIFIER pattern recognition function takes no arguments or 1 argument");
                    }
                    if (node.getArguments().size() == 1) {
                        Node argument = node.getArguments().get(0);
                        if (!(argument instanceof Identifier)) {
                            throw semanticException(TYPE_MISMATCH, argument, "CLASSIFIER function argument should be primary pattern variable or subset name. Actual: %s", argument.getClass().getSimpleName());
                        }
                        Identifier identifier = (Identifier) argument;
                        String label = label(identifier);
                        if (!context.getContext().getLabels().contains(label)) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, argument, "%s is not a primary pattern variable or subset name", identifier.getValue());
                        }
                    }
                    return setExpressionType(node, VARCHAR);
                default:
                    throw new IllegalStateException("unexpected pattern recognition function " + node.getName());
            }
        }

        private void validateNavigationNesting(FunctionCall node)
        {
            checkArgument(isPatternNavigationFunction(node));
            String name = node.getName().getSuffix();

            // It is allowed to nest FIRST and LAST functions within PREV and NEXT functions. Only immediate nesting is supported
            List<FunctionCall> nestedNavigationFunctions = extractExpressions(ImmutableList.of(node.getArguments().get(0)), FunctionCall.class).stream()
                    .filter(this::isPatternNavigationFunction)
                    .collect(toImmutableList());
            if (!nestedNavigationFunctions.isEmpty()) {
                if (name.equalsIgnoreCase("FIRST") || name.equalsIgnoreCase("LAST")) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nestedNavigationFunctions.get(0),
                            "Cannot nest %s pattern navigation function inside %s pattern navigation function", nestedNavigationFunctions.get(0).getName(), name);
                }
                if (nestedNavigationFunctions.size() > 1) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nestedNavigationFunctions.get(1),
                            "Cannot nest multiple pattern navigation functions inside %s pattern navigation function", name);
                }
                FunctionCall nested = getOnlyElement(nestedNavigationFunctions);
                String nestedName = nested.getName().getSuffix();
                if (nestedName.equalsIgnoreCase("PREV") || nestedName.equalsIgnoreCase("NEXT")) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nested,
                            "Cannot nest %s pattern navigation function inside %s pattern navigation function", nestedName, name);
                }
                if (nested != node.getArguments().get(0)) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nested,
                            "Immediate nesting is required for pattern navigation functions");
                }
            }
        }

        /**
         * Check that all aggregation arguments refer consistently to the same label.
         */
        private void validateAggregationLabelConsistency(FunctionCall node)
        {
            if (node.getArguments().isEmpty()) {
                return;
            }

            Set<Optional<String>> argumentLabels = new HashSet<>();
            for (int i = 0; i < node.getArguments().size(); i++) {
                ArgumentLabel argumentLabel = validateLabelConsistency(node, false, i);
                if (argumentLabel.hasLabel()) {
                    argumentLabels.add(argumentLabel.getLabel());
                }
            }
            if (argumentLabels.size() > 1) {
                throw semanticException(INVALID_ARGUMENTS, node, "All aggregate function arguments must apply to rows matched with the same label");
            }
        }

        /**
         * Check that the navigated expression refers consistently to the same label.
         */
        private void validateNavigationLabelConsistency(FunctionCall node)
        {
            checkArgument(isPatternNavigationFunction(node));
            validateLabelConsistency(node, true, 0);
        }

        private ArgumentLabel validateLabelConsistency(FunctionCall node, boolean labelRequired, int argumentIndex)
        {
            String name = node.getName().getSuffix();

            List<Expression> unlabeledInputColumns = Streams.concat(
                            extractExpressions(ImmutableList.of(node.getArguments().get(argumentIndex)), Identifier.class).stream(),
                            extractExpressions(ImmutableList.of(node.getArguments().get(argumentIndex)), DereferenceExpression.class).stream())
                    .filter(expression -> columnReferences.containsKey(NodeRef.of(expression)))
                    .collect(toImmutableList());
            List<Expression> labeledInputColumns = extractExpressions(ImmutableList.of(node.getArguments().get(argumentIndex)), DereferenceExpression.class).stream()
                    .filter(expression -> labelDereferences.containsKey(NodeRef.of(expression)))
                    .collect(toImmutableList());
            List<FunctionCall> classifiers = extractExpressions(ImmutableList.of(node.getArguments().get(argumentIndex)), FunctionCall.class).stream()
                    .filter(this::isClassifierFunction)
                    .collect(toImmutableList());

            // Pattern navigation function must contain at least one column reference or CLASSIFIER() function. There is no such requirement for the argument of an aggregate function.
            if (unlabeledInputColumns.isEmpty() && labeledInputColumns.isEmpty() && classifiers.isEmpty()) {
                if (labelRequired) {
                    throw semanticException(INVALID_ARGUMENTS, node, "Pattern navigation function %s must contain at least one column reference or CLASSIFIER()", name);
                }
                else {
                    return ArgumentLabel.noLabel();
                }
            }

            // Label consistency rules:
            // All column references must be prefixed with the same label.
            // Alternatively, all column references can have no label. In such case they are considered as prefixed with universal row pattern variable.
            // All CLASSIFIER() calls must have the same label or no label, respectively, as their argument.
            if (!unlabeledInputColumns.isEmpty() && !labeledInputColumns.isEmpty()) {
                throw semanticException(
                        INVALID_ARGUMENTS,
                        labeledInputColumns.get(0),
                        "Column references inside argument of function %s must all either be prefixed with the same label or be not prefixed", name);
            }
            Set<String> inputColumnLabels = labeledInputColumns.stream()
                    .map(expression -> labelDereferences.get(NodeRef.of(expression)))
                    .map(LabelPrefixedReference::getLabel)
                    .collect(toImmutableSet());
            if (inputColumnLabels.size() > 1) {
                throw semanticException(
                        INVALID_ARGUMENTS,
                        labeledInputColumns.get(0),
                        "Column references inside argument of function %s must all either be prefixed with the same label or be not prefixed", name);
            }
            Set<Optional<String>> classifierLabels = classifiers.stream()
                    .map(functionCall -> {
                        if (functionCall.getArguments().isEmpty()) {
                            return Optional.<String>empty();
                        }
                        return Optional.of(label((Identifier) functionCall.getArguments().get(0)));
                    })
                    .collect(toImmutableSet());
            if (classifierLabels.size() > 1) {
                throw semanticException(
                        INVALID_ARGUMENTS,
                        node,
                        "CLASSIFIER() calls inside argument of function %s must all either have the same label as the argument or have no arguments", name);
            }
            if (!unlabeledInputColumns.isEmpty() && !classifiers.isEmpty()) {
                if (!getOnlyElement(classifierLabels).equals(Optional.empty())) {
                    throw semanticException(
                            INVALID_ARGUMENTS,
                            node,
                            "Column references inside argument of function %s must all be prefixed with the same label that all CLASSIFIER() calls have as the argument", name);
                }
            }
            if (!labeledInputColumns.isEmpty() && !classifiers.isEmpty()) {
                if (!getOnlyElement(classifierLabels).equals(Optional.of(getOnlyElement(inputColumnLabels)))) {
                    throw semanticException(
                            INVALID_ARGUMENTS,
                            node,
                            "Column references inside argument of function %s must all be prefixed with the same label that all CLASSIFIER() calls have as the argument", name);
                }
            }

            // For aggregate functions: return the label for the current argument to check if all arguments apply to the same set of rows.
            if (!inputColumnLabels.isEmpty()) {
                return ArgumentLabel.explicitLabel(getOnlyElement(inputColumnLabels));
            }
            else if (!classifierLabels.isEmpty()) {
                return getOnlyElement(classifierLabels)
                        .map(ArgumentLabel::explicitLabel)
                        .orElse(ArgumentLabel.universalLabel());
            }
            else if (!unlabeledInputColumns.isEmpty()) {
                return ArgumentLabel.universalLabel();
            }
            return ArgumentLabel.noLabel();
        }

        private boolean isPatternNavigationFunction(FunctionCall node)
        {
            if (!isPatternRecognitionFunction(node)) {
                return false;
            }
            String name = node.getName().getSuffix().toUpperCase(ENGLISH);
            return name.equals("FIRST") ||
                    name.equals("LAST") ||
                    name.equals("PREV") ||
                    name.equals("NEXT");
        }

        private boolean isClassifierFunction(FunctionCall node)
        {
            if (!isPatternRecognitionFunction(node)) {
                return false;
            }
            return node.getName().getSuffix().toUpperCase(ENGLISH).equals("CLASSIFIER");
        }

        private String label(Identifier identifier)
        {
            return identifier.getCanonicalValue();
        }

        private void analyzePatternAggregation(FunctionCall node)
        {
            if (node.getWindow().isPresent()) {
                throw semanticException(NESTED_WINDOW, node, "Cannot use OVER with %s aggregate function in pattern recognition context", node.getName());
            }
            if (node.getFilter().isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "Cannot use FILTER with %s aggregate function in pattern recognition context", node.getName());
            }
            if (node.getOrderBy().isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "Cannot use ORDER BY with %s aggregate function in pattern recognition context", node.getName());
            }
            if (node.isDistinct()) {
                throw semanticException(NOT_SUPPORTED, node, "Cannot use DISTINCT with %s aggregate function in pattern recognition context", node.getName());
            }

            checkNoNestedAggregations(node);
            checkNoNestedNavigations(node);
        }

        private void checkNoNestedAggregations(FunctionCall node)
        {
            extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(function -> plannerContext.getMetadata().isAggregationFunction(function.getName()))
                    .findFirst()
                    .ifPresent(aggregation -> {
                        throw semanticException(
                                NESTED_AGGREGATION,
                                aggregation,
                                "Cannot nest %s aggregate function inside %s function",
                                aggregation.getName(),
                                node.getName());
                    });
        }

        private void checkNoNestedNavigations(FunctionCall node)
        {
            extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(this::isPatternNavigationFunction)
                    .findFirst()
                    .ifPresent(navigation -> {
                        throw semanticException(
                                INVALID_NAVIGATION_NESTING,
                                navigation,
                                "Cannot nest %s pattern navigation function inside %s function",
                                navigation.getName().getSuffix(),
                                node.getName());
                    });
        }

        @Override
        protected Type visitAtTimeZone(AtTimeZone node, StackableAstVisitorContext<Context> context)
        {
            Type valueType = process(node.getValue(), context);
            process(node.getTimeZone(), context);
            if (!(valueType instanceof TimeWithTimeZoneType) && !(valueType instanceof TimestampWithTimeZoneType) && !(valueType instanceof TimeType) && !(valueType instanceof TimestampType)) {
                throw semanticException(TYPE_MISMATCH, node.getValue(), "Type of value must be a time or timestamp with or without time zone (actual %s)", valueType);
            }
            Type resultType = valueType;
            if (valueType instanceof TimeType) {
                resultType = createTimeWithTimeZoneType(((TimeType) valueType).getPrecision());
            }
            else if (valueType instanceof TimestampType) {
                resultType = createTimestampWithTimeZoneType(((TimestampType) valueType).getPrecision());
            }

            return setExpressionType(node, resultType);
        }

        @Override
        protected Type visitCurrentCatalog(CurrentCatalog node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentSchema(CurrentSchema node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentUser(CurrentUser node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentPath(CurrentPath node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitTrim(Trim node, StackableAstVisitorContext<Context> context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();

            argumentTypes.add(process(node.getTrimSource(), context));
            node.getTrimCharacter().ifPresent(trimChar -> argumentTypes.add(process(trimChar, context)));
            List<Type> actualTypes = argumentTypes.build();

            String functionName = node.getSpecification().getFunctionName();
            ResolvedFunction function = plannerContext.getMetadata().resolveFunction(session, QualifiedName.of(functionName), fromTypes(actualTypes));

            List<Type> expectedTypes = function.getSignature().getArgumentTypes();
            checkState(expectedTypes.size() == actualTypes.size(), "wrong argument number in the resolved signature");

            Type actualTrimSourceType = actualTypes.get(0);
            Type expectedTrimSourceType = expectedTypes.get(0);
            coerceType(node.getTrimSource(), actualTrimSourceType, expectedTrimSourceType, "source argument of trim function");

            if (node.getTrimCharacter().isPresent()) {
                Type actualTrimCharType = actualTypes.get(1);
                Type expectedTrimCharType = expectedTypes.get(1);
                coerceType(node.getTrimCharacter().get(), actualTrimCharType, expectedTrimCharType, "trim character argument of trim function");
            }

            accessControl.checkCanExecuteFunction(SecurityContext.of(session), functionName);

            resolvedFunctions.put(NodeRef.of(node), function);

            return setExpressionType(node, function.getSignature().getReturnType());
        }

        @Override
        protected Type visitFormat(Format node, StackableAstVisitorContext<Context> context)
        {
            List<Type> arguments = node.getArguments().stream()
                    .map(expression -> process(expression, context))
                    .collect(toImmutableList());

            if (!(arguments.get(0) instanceof VarcharType)) {
                throw semanticException(TYPE_MISMATCH, node.getArguments().get(0), "Type of first argument to format() must be VARCHAR (actual: %s)", arguments.get(0));
            }

            for (int i = 1; i < arguments.size(); i++) {
                try {
                    plannerContext.getMetadata().resolveFunction(session, QualifiedName.of(FormatFunction.NAME), fromTypes(arguments.get(0), RowType.anonymous(arguments.subList(1, arguments.size()))));
                }
                catch (TrinoException e) {
                    ErrorCode errorCode = e.getErrorCode();
                    if (errorCode.equals(NOT_SUPPORTED.toErrorCode()) || errorCode.equals(OPERATOR_NOT_FOUND.toErrorCode())) {
                        throw semanticException(NOT_SUPPORTED, node.getArguments().get(i), "Type not supported for formatting: %s", arguments.get(i));
                    }
                    throw e;
                }
            }

            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context)
        {
            if (isDescribe) {
                return setExpressionType(node, UNKNOWN);
            }
            if (parameters.size() == 0) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "Query takes no parameters");
            }
            if (node.getPosition() >= parameters.size()) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "Invalid parameter index %s, max value is %s", node.getPosition(), parameters.size() - 1);
            }

            Expression providedValue = parameters.get(NodeRef.of(node));
            if (providedValue == null) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "No value provided for parameter");
            }
            Type resultType = process(providedValue, context);
            return setExpressionType(node, resultType);
        }

        @Override
        protected Type visitExtract(Extract node, StackableAstVisitorContext<Context> context)
        {
            Type type = process(node.getExpression(), context);
            Extract.Field field = node.getField();

            switch (field) {
                case YEAR:
                case MONTH:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !(type.equals(INTERVAL_YEAR_MONTH))) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case DAY:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !(type.equals(INTERVAL_DAY_TIME))) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case QUARTER:
                case WEEK:
                case DAY_OF_MONTH:
                case DAY_OF_WEEK:
                case DOW:
                case DAY_OF_YEAR:
                case DOY:
                case YEAR_OF_WEEK:
                case YOW:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case HOUR:
                case MINUTE:
                case SECOND:
                    if (!(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !(type instanceof TimeType) &&
                            !(type instanceof TimeWithTimeZoneType) &&
                            !(type.equals(INTERVAL_DAY_TIME))) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case TIMEZONE_MINUTE:
                case TIMEZONE_HOUR:
                    if (!(type instanceof TimestampWithTimeZoneType) && !(type instanceof TimeWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown field: " + field);
            }

            return setExpressionType(node, BIGINT);
        }

        private boolean isDateTimeType(Type type)
        {
            return type.equals(DATE) ||
                    type instanceof TimeType ||
                    type instanceof TimeWithTimeZoneType ||
                    type instanceof TimestampType ||
                    type instanceof TimestampWithTimeZoneType ||
                    type.equals(INTERVAL_DAY_TIME) ||
                    type.equals(INTERVAL_YEAR_MONTH);
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, StackableAstVisitorContext<Context> context)
        {
            Type valueType = process(node.getValue(), context);
            Type minType = process(node.getMin(), context);
            Type maxType = process(node.getMax(), context);

            Optional<Type> commonType = typeCoercion.getCommonSuperType(valueType, minType)
                    .flatMap(type -> typeCoercion.getCommonSuperType(type, maxType));

            if (commonType.isEmpty()) {
                semanticException(TYPE_MISMATCH, node, "Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType);
            }

            if (!commonType.get().isOrderable()) {
                semanticException(TYPE_MISMATCH, node, "Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType);
            }

            if (!valueType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(node.getValue(), valueType, commonType.get());
            }
            if (!minType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(node.getMin(), minType, commonType.get());
            }
            if (!maxType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(node.getMax(), maxType, commonType.get());
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        public Type visitTryExpression(TryExpression node, StackableAstVisitorContext<Context> context)
        {
            // TRY is rewritten to lambda, and lambda is not supported in pattern recognition
            if (context.getContext().isPatternRecognition()) {
                throw semanticException(NOT_SUPPORTED, node, "TRY expression in pattern recognition context is not yet supported");
            }

            Type type = process(node.getInnerExpression(), context);
            return setExpressionType(node, type);
        }

        @Override
        public Type visitCast(Cast node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                type = plannerContext.getTypeManager().getType(toTypeSignature(node.getType()));
            }
            catch (TypeNotFoundException e) {
                throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", node.getType());
            }

            if (type.equals(UNKNOWN)) {
                throw semanticException(TYPE_MISMATCH, node, "UNKNOWN is not a valid type");
            }

            Type value = process(node.getExpression(), context);
            if (!value.equals(UNKNOWN) && !node.isTypeOnly()) {
                try {
                    plannerContext.getMetadata().getCoercion(session, value, type);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitInPredicate(InPredicate node, StackableAstVisitorContext<Context> context)
        {
            Expression value = node.getValue();
            Expression valueList = node.getValueList();

            // When an IN-predicate containing a subquery: `x IN (SELECT ...)` is planned, both `value` and `valueList` are pre-planned.
            // In the row pattern matching context, expressions can contain labeled column references, navigations, CALSSIFIER(), and MATCH_NUMBER() calls.
            // None of these can be pre-planned. Instead, the query fails:
            // - if such elements are in the `value list` (subquery), the analysis of the subquery fails as it is done in a non-pattern-matching context.
            // - if such elements are in `value`, they are captured by the below check.
            //
            // NOTE: Theoretically, if the IN-predicate does not contain CLASSIFIER() or MATCH_NUMBER() calls, it could be pre-planned
            // on the condition that all column references of the `value` are consistently navigated (i.e., the expression is effectively evaluated within a single row),
            // and that the same navigation should be applied to the resulting symbol.
            // Currently, we only support the case when there are no explicit labels or navigations. This is a special case of such
            // consistent navigating, as the column reference `x` defaults to `RUNNING LAST(universal_pattern_variable.x)`.
            if (context.getContext().isPatternRecognition() && valueList instanceof SubqueryExpression) {
                extractExpressions(ImmutableList.of(value), FunctionCall.class).stream()
                        .filter(ExpressionAnalyzer::isPatternRecognitionFunction)
                        .findFirst()
                        .ifPresent(function -> {
                            throw semanticException(NOT_SUPPORTED, function, "IN-PREDICATE with %s function is not yet supported", function.getName().getSuffix());
                        });
                extractExpressions(ImmutableList.of(value), DereferenceExpression.class)
                        .forEach(dereference -> {
                            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(dereference);
                            if (qualifiedName != null) {
                                String label = label(qualifiedName.getOriginalParts().get(0));
                                if (context.getContext().getLabels().contains(label)) {
                                    throw semanticException(NOT_SUPPORTED, dereference, "IN-PREDICATE with labeled column reference is not yet supported");
                                }
                            }
                        });
            }

            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;
                Type type = coerceToSingleType(context,
                        "IN value and list items must be the same type: %s",
                        ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build());
                setExpressionType(inListExpression, type);
            }
            else if (valueList instanceof SubqueryExpression) {
                subqueryInPredicates.add(NodeRef.of(node));
                analyzePredicateWithSubquery(node, process(value, context), (SubqueryExpression) valueList, context);
            }
            else {
                throw new IllegalArgumentException("Unexpected value list type for InPredicate: " + node.getValueList().getClass().getName());
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, StackableAstVisitorContext<Context> context)
        {
            Type type = analyzeSubquery(node, context);

            // the implied type of a scalar subquery is that of the unique field in the single-column row
            if (type instanceof RowType && ((RowType) type).getFields().size() == 1) {
                type = type.getTypeParameters().get(0);
            }

            setExpressionType(node, type);
            subqueries.add(NodeRef.of(node));
            return type;
        }

        /**
         * @return the common supertype between the value type and subquery type
         */
        private Type analyzePredicateWithSubquery(Expression node, Type declaredValueType, SubqueryExpression subquery, StackableAstVisitorContext<Context> context)
        {
            Type valueRowType = declaredValueType;
            if (!(declaredValueType instanceof RowType) && !(declaredValueType instanceof UnknownType)) {
                valueRowType = RowType.anonymous(ImmutableList.of(declaredValueType));
            }

            Type subqueryType = analyzeSubquery(subquery, context);
            setExpressionType(subquery, subqueryType);

            Optional<Type> commonType = typeCoercion.getCommonSuperType(valueRowType, subqueryType);

            if (commonType.isEmpty()) {
                throw semanticException(TYPE_MISMATCH, node, "Value expression and result of subquery must be of the same type: %s vs %s", valueRowType, subqueryType);
            }

            Optional<Type> valueCoercion = Optional.empty();
            if (!valueRowType.equals(commonType.get())) {
                valueCoercion = commonType;
            }

            Optional<Type> subQueryCoercion = Optional.empty();
            if (!subqueryType.equals(commonType.get())) {
                subQueryCoercion = commonType;
            }

            predicateCoercions.put(NodeRef.of(node), new PredicateCoercions(valueRowType, valueCoercion, subQueryCoercion));

            return commonType.get();
        }

        private Type analyzeSubquery(SubqueryExpression node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                throw semanticException(NOT_SUPPORTED, node, "Lambda expression cannot contain subqueries");
            }
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node, context.getContext().getCorrelationSupport());
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .build();
            Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            for (int i = 0; i < queryScope.getRelationType().getAllFieldCount(); i++) {
                Field field = queryScope.getRelationType().getFieldByIndex(i);
                if (!field.isHidden()) {
                    if (field.getName().isPresent()) {
                        fields.add(RowType.field(field.getName().get(), field.getType()));
                    }
                    else {
                        fields.add(RowType.field(field.getType()));
                    }
                }
            }

            sourceFields.addAll(queryScope.getRelationType().getVisibleFields());
            return RowType.from(fields.build());
        }

        @Override
        protected Type visitExists(ExistsPredicate node, StackableAstVisitorContext<Context> context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node, context.getContext().getCorrelationSupport());
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .build();

            List<RowType.Field> fields = analyzer.analyze(node.getSubquery(), subqueryScope)
                    .getRelationType()
                    .getAllFields().stream()
                    .map(field -> {
                        if (field.getName().isPresent()) {
                            return RowType.field(field.getName().get(), field.getType());
                        }

                        return RowType.field(field.getType());
                    })
                    .collect(toImmutableList());

            // TODO: this should be multiset(row(...))
            setExpressionType(node.getSubquery(), RowType.from(fields));

            existsSubqueries.add(NodeRef.of(node));

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            quantifiedComparisons.add(NodeRef.of(node));

            Type declaredValueType = process(node.getValue(), context);
            Type comparisonType = analyzePredicateWithSubquery(node, declaredValueType, (SubqueryExpression) node.getSubquery(), context);

            switch (node.getOperator()) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (!comparisonType.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, node, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    if (!comparisonType.isComparable()) {
                        throw semanticException(TYPE_MISMATCH, node, "Type [%s] must be comparable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                default:
                    throw new IllegalStateException(format("Unexpected comparison type: %s", node.getOperator()));
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        public Type visitFieldReference(FieldReference node, StackableAstVisitorContext<Context> context)
        {
            ResolvedField field = baseScope.getField(node.getFieldIndex());
            return handleResolvedField(node, field, context);
        }

        @Override
        protected Type visitLambdaExpression(LambdaExpression node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isPatternRecognition()) {
                throw semanticException(NOT_SUPPORTED, node, "Lambda expression in pattern recognition context is not yet supported");
            }

            verifyNoAggregateWindowOrGroupingFunctions(plannerContext.getMetadata(), node.getBody(), "Lambda expression");
            if (!context.getContext().isExpectingLambda()) {
                throw semanticException(TYPE_MISMATCH, node, "Lambda expression should always be used inside a function");
            }

            List<Type> types = context.getContext().getFunctionInputTypes();
            List<LambdaArgumentDeclaration> lambdaArguments = node.getArguments();

            if (types.size() != lambdaArguments.size()) {
                throw semanticException(INVALID_PARAMETER_USAGE, node,
                        format("Expected a lambda that takes %s argument(s) but got %s", types.size(), lambdaArguments.size()));
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < lambdaArguments.size(); i++) {
                LambdaArgumentDeclaration lambdaArgument = lambdaArguments.get(i);
                Type type = types.get(i);
                fields.add(io.trino.sql.analyzer.Field.newUnqualified(lambdaArgument.getName().getValue(), type));
                setExpressionType(lambdaArgument, type);
            }

            Scope lambdaScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .withRelationType(RelationId.of(node), new RelationType(fields.build()))
                    .build();

            ImmutableMap.Builder<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration = ImmutableMap.builder();
            if (context.getContext().isInLambda()) {
                fieldToLambdaArgumentDeclaration.putAll(context.getContext().getFieldToLambdaArgumentDeclaration());
            }
            for (LambdaArgumentDeclaration lambdaArgument : lambdaArguments) {
                ResolvedField resolvedField = lambdaScope.resolveField(lambdaArgument, QualifiedName.of(lambdaArgument.getName().getValue()));
                fieldToLambdaArgumentDeclaration.put(FieldId.from(resolvedField), lambdaArgument);
            }

            Type returnType = process(node.getBody(), new StackableAstVisitorContext<>(context.getContext().inLambda(lambdaScope, fieldToLambdaArgumentDeclaration.buildOrThrow())));
            FunctionType functionType = new FunctionType(types, returnType);
            return setExpressionType(node, functionType);
        }

        @Override
        protected Type visitBindExpression(BindExpression node, StackableAstVisitorContext<Context> context)
        {
            verify(context.getContext().isExpectingLambda(), "bind expression found when lambda is not expected");

            StackableAstVisitorContext<Context> innerContext = new StackableAstVisitorContext<>(context.getContext().notExpectingLambda());
            ImmutableList.Builder<Type> functionInputTypesBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                functionInputTypesBuilder.add(process(value, innerContext));
            }
            functionInputTypesBuilder.addAll(context.getContext().getFunctionInputTypes());
            List<Type> functionInputTypes = functionInputTypesBuilder.build();

            FunctionType functionType = (FunctionType) process(node.getFunction(), new StackableAstVisitorContext<>(context.getContext().expectingLambda(functionInputTypes)));

            List<Type> argumentTypes = functionType.getArgumentTypes();
            int numCapturedValues = node.getValues().size();
            verify(argumentTypes.size() == functionInputTypes.size());
            for (int i = 0; i < numCapturedValues; i++) {
                verify(functionInputTypes.get(i).equals(argumentTypes.get(i)));
            }

            FunctionType result = new FunctionType(argumentTypes.subList(numCapturedValues, argumentTypes.size()), functionType.getReturnType());
            return setExpressionType(node, result);
        }

        @Override
        protected Type visitExpression(Expression node, StackableAstVisitorContext<Context> context)
        {
            throw semanticException(NOT_SUPPORTED, node, "not yet implemented: %s", node.getClass().getName());
        }

        @Override
        protected Type visitNode(Node node, StackableAstVisitorContext<Context> context)
        {
            throw semanticException(NOT_SUPPORTED, node, "not yet implemented: %s", node.getClass().getName());
        }

        @Override
        public Type visitGroupingOperation(GroupingOperation node, StackableAstVisitorContext<Context> context)
        {
            if (node.getGroupingColumns().size() > MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "GROUPING supports up to %d column arguments", MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT);
            }

            for (Expression columnArgument : node.getGroupingColumns()) {
                process(columnArgument, context);
            }

            if (node.getGroupingColumns().size() <= MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER) {
                return setExpressionType(node, INTEGER);
            }
            else {
                return setExpressionType(node, BIGINT);
            }
        }

        private Type getOperator(StackableAstVisitorContext<Context> context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            BoundSignature operatorSignature;
            try {
                operatorSignature = plannerContext.getMetadata().resolveOperator(session, operatorType, argumentTypes.build()).getSignature();
            }
            catch (OperatorNotFoundException e) {
                throw semanticException(TYPE_MISMATCH, node, e, "%s", e.getMessage());
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                Type type = operatorSignature.getArgumentTypes().get(i);
                coerceType(context, expression, type, format("Operator %s argument %d", operatorSignature, i));
            }

            Type type = operatorSignature.getReturnType();
            return setExpressionType(node, type);
        }

        private void coerceType(Expression expression, Type actualType, Type expectedType, String message)
        {
            if (!actualType.equals(expectedType)) {
                if (!typeCoercion.canCoerce(actualType, expectedType)) {
                    throw semanticException(TYPE_MISMATCH, expression, "%s must evaluate to a %s (actual: %s)", message, expectedType, actualType);
                }
                addOrReplaceExpressionCoercion(expression, actualType, expectedType);
            }
        }

        private void coerceType(StackableAstVisitorContext<Context> context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, Node node, String message, Expression first, Expression second)
        {
            Type firstType = UNKNOWN;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = UNKNOWN;
            if (second != null) {
                secondType = process(second, context);
            }

            // coerce types if possible
            Optional<Type> superTypeOptional = typeCoercion.getCommonSuperType(firstType, secondType);
            if (superTypeOptional.isPresent()
                    && typeCoercion.canCoerce(firstType, superTypeOptional.get())
                    && typeCoercion.canCoerce(secondType, superTypeOptional.get())) {
                Type superType = superTypeOptional.get();
                if (!firstType.equals(superType)) {
                    addOrReplaceExpressionCoercion(first, firstType, superType);
                }
                if (!secondType.equals(superType)) {
                    addOrReplaceExpressionCoercion(second, secondType, superType);
                }
                return superType;
            }

            throw semanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, String message, List<Expression> expressions)
        {
            // determine super type
            Type superType = UNKNOWN;

            ListMultimap<Type, Expression> typeExpressions = ArrayListMultimap.create();
            for (Expression expression : expressions) {
                typeExpressions.put(process(expression, context), expression);
            }

            // We need an explicit copy to avoid ConcurrentModificationException
            Set<Type> types = typeExpressions.keySet();

            for (Type type : types) {
                Optional<Type> newSuperType = typeCoercion.getCommonSuperType(superType, type);
                if (newSuperType.isEmpty()) {
                    throw semanticException(TYPE_MISMATCH, typeExpressions.get(type).get(0), message, superType);
                }
                superType = newSuperType.get();
            }

            // verify all expressions can be coerced to the superType
            for (Type type : types) {
                List<Expression> coercionCandidates = typeExpressions.get(type);

                if (!type.equals(superType)) {
                    if (!typeCoercion.canCoerce(type, superType)) {
                        throw semanticException(TYPE_MISMATCH, coercionCandidates.get(0), message, superType);
                    }
                    addOrReplaceExpressionsCoercion(coercionCandidates, type, superType);
                }
            }

            return superType;
        }

        private void addOrReplaceExpressionCoercion(Expression expression, Type type, Type superType)
        {
            addOrReplaceExpressionsCoercion(List.of(expression), type, superType);
        }

        private void addOrReplaceExpressionsCoercion(List<Expression> expressions, Type type, Type superType)
        {
            Map<NodeRef<Expression>, Type> expressionRefTypes = expressions.stream()
                    .collect(toImmutableMap(NodeRef::of, expression -> superType));

            expressionCoercions.putAll(expressionRefTypes);
            if (typeCoercion.isTypeOnlyCoercion(type, superType)) {
                typeOnlyCoercions.addAll(expressionRefTypes.keySet());
            }
            else {
                expressionRefTypes.keySet().forEach(typeOnlyCoercions::remove);
            }
        }
    }

    private static class Context
    {
        private final Scope scope;

        // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

        // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
        // Empty list represents expecting a lambda with no arguments.
        private final List<Type> functionInputTypes;
        // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
        // Empty map means that the all lambda expressions surrounding the current node has no arguments.
        private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

        // Primary row pattern variables and named unions (subsets) of variables
        // necessary for the analysis of expressions in the context of row pattern recognition
        private final Set<String> labels;

        private final CorrelationSupport correlationSupport;

        private Context(
                Scope scope,
                List<Type> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration,
                Set<String> labels,
                CorrelationSupport correlationSupport)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
            this.labels = labels;
            this.correlationSupport = requireNonNull(correlationSupport, "correlationSupport is null");
        }

        public static Context notInLambda(Scope scope, CorrelationSupport correlationSupport)
        {
            return new Context(scope, null, null, null, correlationSupport);
        }

        public Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"), labels, correlationSupport);
        }

        public Context expectingLambda(List<Type> functionInputTypes)
        {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration, labels, correlationSupport);
        }

        public Context notExpectingLambda()
        {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration, labels, correlationSupport);
        }

        public static Context patternRecognition(Scope scope, Set<String> labels)
        {
            return new Context(scope, null, null, requireNonNull(labels, "labels is null"), CorrelationSupport.DISALLOWED);
        }

        public Context patternRecognition(Set<String> labels)
        {
            return new Context(scope, functionInputTypes, fieldToLambdaArgumentDeclaration, requireNonNull(labels, "labels is null"), CorrelationSupport.DISALLOWED);
        }

        public Context notExpectingLabels()
        {
            return new Context(scope, functionInputTypes, fieldToLambdaArgumentDeclaration, null, correlationSupport);
        }

        Scope getScope()
        {
            return scope;
        }

        public boolean isInLambda()
        {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isExpectingLambda()
        {
            return functionInputTypes != null;
        }

        public boolean isPatternRecognition()
        {
            return labels != null;
        }

        public Map<FieldId, LambdaArgumentDeclaration> getFieldToLambdaArgumentDeclaration()
        {
            checkState(isInLambda());
            return fieldToLambdaArgumentDeclaration;
        }

        public List<Type> getFunctionInputTypes()
        {
            checkState(isExpectingLambda());
            return functionInputTypes;
        }

        public Set<String> getLabels()
        {
            checkState(isPatternRecognition());
            return labels;
        }

        public CorrelationSupport getCorrelationSupport()
        {
            return correlationSupport;
        }
    }

    public static boolean isPatternRecognitionFunction(FunctionCall node)
    {
        QualifiedName qualifiedName = node.getName();
        if (qualifiedName.getParts().size() > 1) {
            return false;
        }
        Identifier identifier = qualifiedName.getOriginalParts().get(0);
        if (identifier.isDelimited()) {
            return false;
        }
        String name = identifier.getValue().toUpperCase(ENGLISH);
        return name.equals("FIRST") ||
                name.equals("LAST") ||
                name.equals("PREV") ||
                name.equals("NEXT") ||
                name.equals("CLASSIFIER") ||
                name.equals("MATCH_NUMBER");
    }

    public static ExpressionAnalysis analyzePatternRecognitionExpression(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            Expression expression,
            WarningCollector warningCollector,
            Set<String> labels)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        analyzer.analyze(expression, scope, labels);

        updateAnalysis(analysis, analyzer, session, accessControl);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpressions(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            TypeProvider types,
            Iterable<Expression> expressions,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            QueryType queryType)
    {
        return analyzeExpressions(
                new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, new Analysis(null, parameters, queryType), session, types, warningCollector),
                expressions);
    }

    public static ExpressionAnalysis analyzeExpressions(ExpressionAnalyzer analyzer, Iterable<Expression> expressions)
    {
        for (Expression expression : expressions) {
            analyzer.analyze(
                    expression,
                    Scope.builder()
                            .withRelationType(RelationId.anonymous(), new RelationType())
                            .build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            Expression expression,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        analyzer.analyze(expression, scope, correlationSupport);

        updateAnalysis(analysis, analyzer, session, accessControl);
        analysis.addExpressionFields(expression, analyzer.getSourceFields());

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeWindow(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport,
            ResolvedWindow window,
            Node originalNode)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        analyzer.analyzeWindow(window, scope, originalNode, correlationSupport);

        updateAnalysis(analysis, analyzer, session, accessControl);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    private static void updateAnalysis(Analysis analysis, ExpressionAnalyzer analyzer, Session session, AccessControl accessControl)
    {
        analysis.addTypes(analyzer.getExpressionTypes());
        analysis.addCoercions(
                analyzer.getExpressionCoercions(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getSortKeyCoercionsForFrameBoundCalculation(),
                analyzer.getSortKeyCoercionsForFrameBoundComparison());
        analysis.addFrameBoundCalculations(analyzer.getFrameBoundCalculations());
        analyzer.getResolvedFunctions().forEach((key, value) -> analysis.addResolvedFunction(key.getNode(), value, session.getUser()));
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());
        analysis.addLabelDereferences(analyzer.getLabelDereferences());
        analysis.addPatternRecognitionFunctions(analyzer.getPatternRecognitionFunctions());
        analysis.setRanges(analyzer.getRanges());
        analysis.setUndefinedLabels(analyzer.getUndefinedLabels());
        analysis.setMeasureDefinitions(analyzer.getMeasureDefinitions());
        analysis.setPatternAggregations(analyzer.getPatternAggregations());
        analysis.addPredicateCoercions(analyzer.getPredicateCoercions());
    }

    public static ExpressionAnalyzer createConstantAnalyzer(
            PlannerContext plannerContext,
            AccessControl accessControl,
            Session session,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector)
    {
        return createWithoutSubqueries(
                plannerContext,
                accessControl,
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                false);
    }

    public static ExpressionAnalyzer createConstantAnalyzer(
            PlannerContext plannerContext,
            AccessControl accessControl,
            Session session,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                plannerContext,
                accessControl,
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            PlannerContext plannerContext,
            AccessControl accessControl,
            Session session,
            Map<NodeRef<Parameter>, Expression> parameters,
            ErrorCodeSupplier errorCode,
            String message,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                plannerContext,
                accessControl,
                session,
                TypeProvider.empty(),
                parameters,
                node -> semanticException(errorCode, node, message),
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            PlannerContext plannerContext,
            AccessControl accessControl,
            Session session,
            TypeProvider symbolTypes,
            Map<NodeRef<Parameter>, Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return new ExpressionAnalyzer(
                plannerContext,
                accessControl,
                (node, correlationSupport) -> {
                    throw statementAnalyzerRejection.apply(node);
                },
                session,
                symbolTypes,
                parameters,
                warningCollector,
                isDescribe,
                expression -> {
                    throw new IllegalStateException("Cannot access preanalyzed types");
                },
                functionCall -> {
                    throw new IllegalStateException("Cannot access resolved windows");
                });
    }

    public static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                type instanceof DecimalType;
    }

    private static boolean isExactNumericWithScaleZero(Type type)
    {
        return type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type instanceof DecimalType && ((DecimalType) type).getScale() == 0;
    }

    public static class LabelPrefixedReference
    {
        private final String label;
        private final Optional<Identifier> column;

        public LabelPrefixedReference(String label, Identifier column)
        {
            this(label, Optional.of(requireNonNull(column, "column is null")));
        }

        public LabelPrefixedReference(String label)
        {
            this(label, Optional.empty());
        }

        private LabelPrefixedReference(String label, Optional<Identifier> column)
        {
            this.label = requireNonNull(label, "label is null");
            this.column = requireNonNull(column, "column is null");
        }

        public String getLabel()
        {
            return label;
        }

        public Optional<Identifier> getColumn()
        {
            return column;
        }
    }

    private static class ArgumentLabel
    {
        private final boolean hasLabel;
        private final Optional<String> label;

        private ArgumentLabel(boolean hasLabel, Optional<String> label)
        {
            this.hasLabel = hasLabel;
            this.label = label;
        }

        public static ArgumentLabel noLabel()
        {
            return new ArgumentLabel(false, Optional.empty());
        }

        public static ArgumentLabel universalLabel()
        {
            return new ArgumentLabel(true, Optional.empty());
        }

        public static ArgumentLabel explicitLabel(String label)
        {
            return new ArgumentLabel(true, Optional.of(label));
        }

        public boolean hasLabel()
        {
            return hasLabel;
        }

        public Optional<String> getLabel()
        {
            checkState(hasLabel, "no label available");
            return label;
        }
    }
}
