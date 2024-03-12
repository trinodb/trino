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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.ArrayConstructor;
import io.trino.operator.scalar.FormatFunction;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
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
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis.PredicateCoercions;
import io.trino.sql.analyzer.Analysis.Range;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.JsonPathAnalyzer.JsonPathAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.AggregationDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ClassifierDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.MatchNumberDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.Navigation;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationMode;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.PatternInputAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ScalarInputDescriptor;
import io.trino.sql.planner.LiteralInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
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
import io.trino.sql.tree.DataType;
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
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonPathParameter.JsonFormat;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
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
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.type.FunctionType;
import io.trino.type.JsonPath2016Type;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.json.JsonArrayFunction.JSON_ARRAY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonExistsFunction.JSON_EXISTS_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF16_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF32_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF8_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARCHAR_TO_JSON;
import static io.trino.operator.scalar.json.JsonObjectFunction.JSON_OBJECT_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF16;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF32;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF8;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARCHAR;
import static io.trino.operator.scalar.json.JsonQueryFunction.JSON_QUERY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonValueFunction.JSON_VALUE_FUNCTION_NAME;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.DUPLICATE_PARAMETER_NAME;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.INVALID_NAVIGATION_NESTING;
import static io.trino.spi.StandardErrorCode.INVALID_ORDER_BY;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.INVALID_PATH;
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
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
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
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.FIRST;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.LAST;
import static io.trino.sql.analyzer.SemanticExceptions.missingAttributeException;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.tree.DereferenceExpression.isQualifiedAllFieldsReference;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.PRECEDING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior.CONDITIONAL;
import static io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior.UNCONDITIONAL;
import static io.trino.sql.tree.JsonValue.EmptyOrErrorBehavior.DEFAULT;
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
import static io.trino.type.Json2016Type.JSON_2016;
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

    private static final CatalogSchemaFunctionName ARRAY_CONSTRUCTOR_NAME = builtinFunctionName(ArrayConstructor.NAME);

    public static final RowType JSON_NO_PARAMETERS_ROW_TYPE = RowType.anonymous(ImmutableList.of(UNKNOWN));

    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory;
    private final LiteralInterpreter literalInterpreter;
    private final TypeProvider symbolTypes;
    private final boolean isDescribe;

    // Cache from SQL type name to Type; every Type in the cache has a CAST defined from VARCHAR
    private final Cache<String, Type> varcharCastableTypeCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

    private final Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions = new LinkedHashMap<>();
    private final Set<NodeRef<SubqueryExpression>> subqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();

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
    private final Map<NodeRef<Expression>, Optional<String>> labels = new HashMap<>();
    // Record functions specific to row pattern recognition context
    private final Map<NodeRef<RangeQuantifier>, Range> ranges = new LinkedHashMap<>();
    private final Map<NodeRef<RowPattern>, Set<String>> undefinedLabels = new LinkedHashMap<>();
    private final Map<NodeRef<Identifier>, String> resolvedLabels = new LinkedHashMap<>();
    private final Map<NodeRef<SubsetDefinition>, Set<String>> subsets = new LinkedHashMap<>();
    private final Map<NodeRef<WindowOperation>, MeasureDefinition> measureDefinitions = new LinkedHashMap<>();

    // Pattern function analysis (classifier, match_number, aggregations and prev/next/first/last) in the context of the given node
    private final Map<NodeRef<Expression>, List<PatternInputAnalysis>> patternRecognitionInputs = new LinkedHashMap<>();

    private final Set<NodeRef<FunctionCall>> patternNavigationFunctions = new LinkedHashSet<>();

    // for JSON functions
    private final Map<NodeRef<Node>, JsonPathAnalysis> jsonPathAnalyses = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedFunction> jsonInputFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, ResolvedFunction> jsonOutputFunctions = new LinkedHashMap<>();

    private final Session session;
    private final Map<NodeRef<Parameter>, Expression> parameters;
    private final WarningCollector warningCollector;
    private final TypeCoercion typeCoercion;
    private final Function<Expression, Type> getPreanalyzedType;
    private final Function<Node, ResolvedWindow> getResolvedWindow;
    private final List<Field> sourceFields = new ArrayList<>();
    private final FunctionResolver functionResolver;

    private ExpressionAnalyzer(
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
        this.literalInterpreter = new LiteralInterpreter(plannerContext, session);
        this.session = requireNonNull(session, "session is null");
        this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        this.getPreanalyzedType = requireNonNull(getPreanalyzedType, "getPreanalyzedType is null");
        this.getResolvedWindow = requireNonNull(getResolvedWindow, "getResolvedWindow is null");
        this.functionResolver = plannerContext.getFunctionResolver(warningCollector);
    }

    public Map<NodeRef<Node>, ResolvedFunction> getResolvedFunctions()
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

        patternRecognitionInputs.put(NodeRef.of(expression), visitor.getPatternRecognitionInputs());

        return visitor.process(expression, Context.notInLambda(scope, CorrelationSupport.ALLOWED));
    }

    public Type analyze(Expression expression, Scope scope, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);

        patternRecognitionInputs.put(NodeRef.of(expression), visitor.getPatternRecognitionInputs());

        return visitor.process(expression, Context.notInLambda(scope, correlationSupport));
    }

    private Type analyze(Expression expression, Scope scope, Set<String> labels, boolean inWindow)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        Type type = visitor.process(expression, Context.patternRecognition(scope, labels, inWindow));

        patternRecognitionInputs.put(NodeRef.of(expression), visitor.getPatternRecognitionInputs());

        return type;
    }

    private Type analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope, warningCollector);

        patternRecognitionInputs.put(NodeRef.of(expression), visitor.getPatternRecognitionInputs());

        return visitor.process(expression, context);
    }

    private RowType analyzeJsonPathInvocation(JsonTable node, Scope scope, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        List<Type> inputTypes = visitor.analyzeJsonPathInvocation("JSON_TABLE", node, node.getJsonPathInvocation(), Context.notInLambda(scope, correlationSupport));
        return (RowType) inputTypes.get(2);
    }

    private Type analyzeJsonValueExpression(ValueColumn column, JsonPathAnalysis pathAnalysis, Scope scope, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        List<Type> pathInvocationArgumentTypes = ImmutableList.of(JSON_2016, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), JSON_NO_PARAMETERS_ROW_TYPE);
        return visitor.analyzeJsonValueExpression(
                column,
                pathAnalysis,
                Optional.of(column.getType()),
                pathInvocationArgumentTypes,
                column.getEmptyBehavior(),
                column.getEmptyDefault(),
                column.getErrorBehavior(),
                column.getErrorDefault(),
                Context.notInLambda(scope, correlationSupport));
    }

    private Type analyzeJsonQueryExpression(QueryColumn column, Scope scope)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        List<Type> pathInvocationArgumentTypes = ImmutableList.of(JSON_2016, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), JSON_NO_PARAMETERS_ROW_TYPE);
        return visitor.analyzeJsonQueryExpression(
                column,
                column.getWrapperBehavior(),
                column.getQuotesBehavior(),
                pathInvocationArgumentTypes,
                Optional.of(column.getType()),
                Optional.of(column.getFormat()));
    }

    private void analyzeWindow(ResolvedWindow window, Scope scope, Node originalNode, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        visitor.analyzeWindow(window, Context.inWindow(scope, correlationSupport), originalNode);
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

    public Map<NodeRef<Expression>, Optional<String>> getLabels()
    {
        return labels;
    }

    public Map<NodeRef<RangeQuantifier>, Range> getRanges()
    {
        return ranges;
    }

    public Map<NodeRef<RowPattern>, Set<String>> getUndefinedLabels()
    {
        return undefinedLabels;
    }

    public Map<NodeRef<Identifier>, String> getResolvedLabels()
    {
        return resolvedLabels;
    }

    public Map<NodeRef<SubsetDefinition>, Set<String>> getSubsetLabels()
    {
        return subsets;
    }

    public Map<NodeRef<WindowOperation>, MeasureDefinition> getMeasureDefinitions()
    {
        return measureDefinitions;
    }

    public Map<NodeRef<Expression>, List<PatternInputAnalysis>> getPatternRecognitionInputs()
    {
        return patternRecognitionInputs;
    }

    public Set<NodeRef<FunctionCall>> getPatternNavigationFunctions()
    {
        return patternNavigationFunctions;
    }

    public Map<NodeRef<Node>, JsonPathAnalysis> getJsonPathAnalyses()
    {
        return jsonPathAnalyses;
    }

    public Map<NodeRef<Expression>, ResolvedFunction> getJsonInputFunctions()
    {
        return jsonInputFunctions;
    }

    public Map<NodeRef<Node>, ResolvedFunction> getJsonOutputFunctions()
    {
        return jsonOutputFunctions;
    }

    private class Visitor
            extends AstVisitor<Type, Context>
    {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;
        private final WarningCollector warningCollector;

        private final List<PatternInputAnalysis> patternRecognitionInputs = new ArrayList<>();

        public Visitor(Scope baseScope, WarningCollector warningCollector)
        {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        public List<PatternInputAnalysis> getPatternRecognitionInputs()
        {
            return patternRecognitionInputs;
        }

        @Override
        public Type process(Node node, @Nullable Context context)
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
        protected Type visitRow(Row node, Context context)
        {
            List<Type> types = node.getItems().stream()
                    .map(child -> process(child, context))
                    .collect(toImmutableList());

            Type type = RowType.anonymous(types);
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitCurrentDate(CurrentDate node, Context context)
        {
            return setExpressionType(node, DATE);
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, Context context)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimeWithTimeZoneType::createTimeWithTimeZoneType)
                            .orElse(TIME_TZ_MILLIS));
        }

        @Override
        protected Type visitCurrentTimestamp(CurrentTimestamp node, Context context)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimestampWithTimeZoneType::createTimestampWithTimeZoneType)
                            .orElse(TIMESTAMP_TZ_MILLIS));
        }

        @Override
        protected Type visitLocalTime(LocalTime node, Context context)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimeType::createTimeType)
                            .orElse(TIME_MILLIS));
        }

        @Override
        protected Type visitLocalTimestamp(LocalTimestamp node, Context context)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimestampType::createTimestampType)
                            .orElse(TIMESTAMP_MILLIS));
        }

        @Override
        protected Type visitSymbolReference(SymbolReference node, Context context)
        {
            if (context.isInLambda()) {
                Optional<ResolvedField> resolvedField = context.getScope().tryResolveField(node, QualifiedName.of(node.getName()));
                if (resolvedField.isPresent() && context.getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
                    return setExpressionType(node, resolvedField.get().getType());
                }
            }
            Type type = symbolTypes.get(Symbol.from(node));
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitIdentifier(Identifier node, Context context)
        {
            ResolvedField resolvedField = context.getScope().resolveField(node, QualifiedName.of(node.getValue()));

            if (context.isPatternRecognition()) {
                labels.put(NodeRef.of(node), Optional.empty());
                patternRecognitionInputs.add(new PatternInputAnalysis(
                        node,
                        new ScalarInputDescriptor(Optional.empty(), context.getPatternRecognitionContext().navigation())));
            }

            return handleResolvedField(node, resolvedField, context);
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, Context context)
        {
            if (!resolvedField.isLocal() && context.getCorrelationSupport() != CorrelationSupport.ALLOWED) {
                throw semanticException(NOT_SUPPORTED, node, "Reference to column '%s' from outer scope not allowed in this context", node);
            }

            FieldId fieldId = FieldId.from(resolvedField);
            Field field = resolvedField.getField();
            if (context.isInLambda()) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getFieldToLambdaArgumentDeclaration().get(fieldId);
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
        protected Type visitDereferenceExpression(DereferenceExpression node, Context context)
        {
            if (isQualifiedAllFieldsReference(node)) {
                throw semanticException(NOT_SUPPORTED, node, "<identifier>.* not allowed in this context");
            }

            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                // In the context of row pattern matching, fields are optionally prefixed with labels. Labels are irrelevant during type analysis.
                if (context.isPatternRecognition()) {
                    String label = label(qualifiedName.getOriginalParts().getFirst());
                    if (context.getPatternRecognitionContext().labels().contains(label)) {
                        // In the context of row pattern matching, the name of row pattern input table cannot be used to qualify column names.
                        // (it can only be accessed in PARTITION BY and ORDER BY clauses of MATCH_RECOGNIZE). Consequentially, if a dereference
                        // expression starts with a label, the next part must be a column.
                        // Only strict column references can be prefixed by label. Labeled references to row fields are not supported.
                        QualifiedName unlabeledName = QualifiedName.of(qualifiedName.getOriginalParts().subList(1, qualifiedName.getOriginalParts().size()));
                        if (qualifiedName.getOriginalParts().size() > 2) {
                            throw semanticException(COLUMN_NOT_FOUND, node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        Optional<ResolvedField> resolvedField = context.getScope().tryResolveField(node, unlabeledName);
                        if (resolvedField.isEmpty()) {
                            throw semanticException(COLUMN_NOT_FOUND, node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        // Correlation is not allowed in pattern recognition context. Visitor's context for pattern recognition has CorrelationSupport.DISALLOWED,
                        // and so the following call should fail if the field is from outer scope.

                        labels.put(NodeRef.of(node), Optional.of(label));
                        patternRecognitionInputs.add(new PatternInputAnalysis(
                                node,
                                new ScalarInputDescriptor(Optional.of(label), context.getPatternRecognitionContext().navigation())));

                        return handleResolvedField(node, resolvedField.get(), context);
                    }
                    // In the context of row pattern matching, qualified column references are not allowed.
                    throw missingAttributeException(node, qualifiedName);
                }

                Scope scope = context.getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                if (!scope.isColumnReference(qualifiedName)) {
                    throw missingAttributeException(node, qualifiedName);
                }
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType rowType)) {
                throw semanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            Identifier field = node.getField().orElseThrow();
            String fieldName = field.getValue();

            boolean foundFieldName = false;
            Type rowFieldType = null;
            for (RowType.Field rowField : rowType.getFields()) {
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    if (foundFieldName) {
                        throw semanticException(AMBIGUOUS_NAME, field, "Ambiguous row field reference: %s", fieldName);
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
        protected Type visitNotExpression(NotExpression node, Context context)
        {
            coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitLogicalExpression(LogicalExpression node, Context context)
        {
            for (Expression term : node.getTerms()) {
                coerceType(context, term, BOOLEAN, "Logical expression term");
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, Context context)
        {
            OperatorType operatorType = switch (node.getOperator()) {
                case EQUAL, NOT_EQUAL -> OperatorType.EQUAL;
                case LESS_THAN, GREATER_THAN -> OperatorType.LESS_THAN;
                case LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL -> OperatorType.LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM -> OperatorType.IS_DISTINCT_FROM;
            };

            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, Context context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Context context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, Context context)
        {
            Type firstType = process(node.getFirst(), context);
            Type secondType = process(node.getSecond(), context);

            if (typeCoercion.getCommonSuperType(firstType, secondType).isEmpty()) {
                throw semanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            return setExpressionType(node, firstType);
        }

        @Override
        protected Type visitIfExpression(IfExpression node, Context context)
        {
            coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, Context context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
            }

            Type type = coerceToSingleType(context,
                    "All CASE results",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context)
        {
            coerceCaseOperandToToSingleType(node, context);

            Type type = coerceToSingleType(context,
                    "All CASE results",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private void coerceCaseOperandToToSingleType(SimpleCaseExpression node, Context context)
        {
            Type operandType = process(node.getOperand(), context);

            List<WhenClause> whenClauses = node.getWhenClauses();
            List<Type> whenOperandTypes = new ArrayList<>(whenClauses.size());

            Type commonType = operandType;
            for (WhenClause whenClause : whenClauses) {
                Expression whenOperand = whenClause.getOperand();
                Type whenOperandType = process(whenOperand, context);
                whenOperandTypes.add(whenOperandType);

                commonType = typeCoercion.getCommonSuperType(commonType, whenOperandType)
                        .orElseThrow(() -> semanticException(TYPE_MISMATCH, whenOperand, "CASE operand type does not match WHEN clause operand type: %s vs %s", operandType, whenOperandType));
            }

            if (commonType != operandType) {
                addOrReplaceExpressionCoercion(node.getOperand(), commonType);
            }

            for (int i = 0; i < whenOperandTypes.size(); i++) {
                Type whenOperandType = whenOperandTypes.get(i);
                if (!whenOperandType.equals(commonType)) {
                    Expression whenOperand = whenClauses.get(i).getOperand();
                    addOrReplaceExpressionCoercion(whenOperand, commonType);
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
        protected Type visitCoalesceExpression(CoalesceExpression node, Context context)
        {
            Type type = coerceToSingleType(context, "All COALESCE operands", node.getOperands());

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, Context context)
        {
            return switch (node.getSign()) {
                case PLUS -> {
                    Type type = process(node.getValue(), context);

                    if (!type.equals(DOUBLE) && !type.equals(REAL) && !type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw semanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    yield setExpressionType(node, type);
                }
                case MINUS -> getOperator(context, node, OperatorType.NEGATION, node.getValue());
            };
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, Context context)
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
        protected Type visitSubscriptExpression(SubscriptExpression node, Context context)
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
                int indexValue = toIntExact(((LongLiteral) node.getIndex()).getParsedValue());
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
        protected Type visitArray(Array node, Context context)
        {
            Type type = coerceToSingleType(context, "All ARRAY elements", node.getValues());
            Type arrayType = plannerContext.getTypeManager().getParameterizedType(ARRAY.getName(), ImmutableList.of(TypeSignatureParameter.typeParameter(type.getTypeSignature())));
            return setExpressionType(node, arrayType);
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, Context context)
        {
            VarcharType type = VarcharType.createVarcharType(node.length());
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitBinaryLiteral(BinaryLiteral node, Context context)
        {
            return setExpressionType(node, VARBINARY);
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, Context context)
        {
            if (node.getParsedValue() >= Integer.MIN_VALUE && node.getParsedValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, Context context)
        {
            return setExpressionType(node, DOUBLE);
        }

        @Override
        protected Type visitDecimalLiteral(DecimalLiteral node, Context context)
        {
            DecimalParseResult parseResult;
            try {
                parseResult = Decimals.parse(node.getValue());
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid DECIMAL literal", node.getValue());
            }
            return setExpressionType(node, parseResult.getType());
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, Context context)
        {
            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitGenericLiteral(GenericLiteral node, Context context)
        {
            return setExpressionType(
                    node,
                    switch (node.getType()) {
                        case String value when value.equalsIgnoreCase("CHAR") -> CharType.createCharType(node.getValue().length());
                        case String value when value.equalsIgnoreCase("TIME") -> processTimeLiteral(node);
                        case String value when value.equalsIgnoreCase("TIMESTAMP") -> processTimestampLiteral(node);
                        default -> {
                            Type type = uncheckedCacheGet(varcharCastableTypeCache, node.getType(), () -> {
                                Type resolvedType;
                                try {
                                    resolvedType = plannerContext.getTypeManager().fromSqlType(node.getType());
                                }
                                catch (TypeNotFoundException e) {
                                    throw semanticException(TYPE_NOT_FOUND, node, "Unknown resolvedType: %s", node.getType());
                                }

                                if (!JSON.equals(resolvedType)) {
                                    try {
                                        plannerContext.getMetadata().getCoercion(VARCHAR, resolvedType);
                                    }
                                    catch (IllegalArgumentException e) {
                                        throw semanticException(INVALID_LITERAL, node, "No literal form for resolvedType %s", resolvedType);
                                    }
                                }
                                return resolvedType;
                            });
                            try {
                                literalInterpreter.evaluate(node, type);
                            }
                            catch (RuntimeException e) {
                                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid %s literal", node.getValue(), type.getDisplayName().toUpperCase(ENGLISH));
                            }

                            yield type;
                        }
                    });
        }

        private Type processTimeLiteral(GenericLiteral node)
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
                throw semanticException(INVALID_LITERAL, node, "'%s' is not a valid TIME literal", node.getValue());
            }

            return type;
        }

        private Type processTimestampLiteral(GenericLiteral node)
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
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid TIMESTAMP literal", node.getValue());
            }

            return type;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, Context context)
        {
            Type type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH;
            }
            else {
                type = INTERVAL_DAY_TIME;
            }
            try {
                literalInterpreter.evaluate(node, type);
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid INTERVAL literal", node.getValue());
            }
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, Context context)
        {
            return setExpressionType(node, UNKNOWN);
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, Context context)
        {
            boolean isAggregation = functionResolver.isAggregationFunction(session, node.getName(), accessControl);
            boolean isRowPatternCount = context.isPatternRecognition() &&
                    isAggregation &&
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

            if (context.isPatternRecognition()) {
                if (isPatternRecognitionFunction(node)) {
                    validatePatternRecognitionFunction(node);

                    String name = node.getName().getSuffix().toUpperCase(ENGLISH);
                    return setExpressionType(node, switch (name) {
                        case "MATCH_NUMBER" -> analyzeMatchNumber(node, context);
                        case "CLASSIFIER" -> analyzeClassifier(node, context);
                        case "FIRST", "LAST" -> analyzeLogicalNavigation(node, context, name);
                        case "PREV", "NEXT" -> analyzePhysicalNavigation(node, context, name);
                        default -> throw new IllegalStateException("unexpected pattern recognition function " + name);
                    });
                }
                else if (isAggregation) {
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
                }
            }

            if (node.getProcessingMode().isPresent()) {
                ProcessingMode processingMode = node.getProcessingMode().get();
                if (!context.isPatternRecognition()) {
                    throw semanticException(INVALID_PROCESSING_MODE, processingMode, "%s semantics is not supported out of pattern recognition context", processingMode.getMode());
                }
                if (!isAggregation) {
                    throw semanticException(INVALID_PROCESSING_MODE, processingMode, "%s semantics is supported only for FIRST(), LAST() and aggregation functions. Actual: %s", processingMode.getMode(), node.getName());
                }
            }

            if (node.getWindow().isPresent()) {
                ResolvedWindow window = getResolvedWindow.apply(node);
                checkState(window != null, "no resolved window for: " + node);

                analyzeWindow(window, context.inWindow(), (Node) node.getWindow().get());
                windowFunctions.add(NodeRef.of(node));
            }
            else {
                if (node.isDistinct() && !isAggregation) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "DISTINCT is not supported for non-aggregation functions");
                }
            }

            if (node.getFilter().isPresent()) {
                Expression expression = node.getFilter().get();
                Type type = process(expression, context);
                coerceType(expression, type, BOOLEAN, "Filter expression");
            }

            List<TypeSignatureProvider> argumentTypes = getCallArgumentTypes(node.getArguments(), context);

            if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                // Due to fact that the LISTAGG function is transformed out of pragmatic reasons
                // in a synthetic function call, the type expression of this function call is evaluated
                // explicitly here in order to make sure that it is a varchar.
                List<Expression> arguments = node.getArguments();
                Expression expression = arguments.getFirst();
                Type expressionType = process(expression, context);
                if (!(expressionType instanceof VarcharType)) {
                    throw semanticException(TYPE_MISMATCH, node, "Expected expression of varchar, but '%s' has %s type", expression, expressionType.getDisplayName());
                }
            }

            ResolvedFunction function;
            try {
                function = functionResolver.resolveFunction(session, node.getName(), argumentTypes, accessControl);
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

            if (function.getSignature().getName().equals(ARRAY_CONSTRUCTOR_NAME)) {
                // After optimization, array constructor is rewritten to a function call.
                // For historic reasons array constructor is allowed to have 254 arguments
                if (node.getArguments().size() > 254) {
                    throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for array constructor");
                }
            }
            else if (node.getArguments().size() > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for function call %s()", function.getSignature().getName().getFunctionName());
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
                if (expectedType == null) {
                    throw new NullPointerException(format("Type '%s' not found", signature.getArgumentTypes().get(i)));
                }
                if (node.isDistinct() && !expectedType.isComparable()) {
                    throw semanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
                }
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, context.expectingLambda(expectedFunctionType.getArgumentTypes()));
                }
                else {
                    Type actualType = plannerContext.getTypeManager().getType(argumentTypes.get(i).getTypeSignature());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            // must run after arguments are processed and labels are recorded
            if (context.isPatternRecognition() && isAggregation) {
                analyzePatternAggregation(node, function);
            }

            Type type = signature.getReturnType();
            return setExpressionType(node, type);
        }

        private void analyzeWindow(ResolvedWindow window, Context context, Node originalNode)
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
                throw semanticException(NESTED_WINDOW, nestedWindowExpressions.getFirst(), "Cannot nest window functions or row pattern measures inside window specification");
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

                    frame.getAfterMatchSkipTo()
                            .flatMap(SkipTo::getIdentifier)
                            .ifPresent(label -> resolvedLabels.put(NodeRef.of(label), label.getCanonicalValue()));

                    for (SubsetDefinition subset : frame.getSubsets()) {
                        resolvedLabels.put(NodeRef.of(subset.getName()), subset.getName().getCanonicalValue());
                        subsets.put(
                                NodeRef.of(subset),
                                subset.getIdentifiers().stream()
                                        .map(Identifier::getCanonicalValue)
                                        .collect(Collectors.toSet()));
                    }

                    ranges.putAll(analysis.ranges());
                    undefinedLabels.put(NodeRef.of(frame.getPattern().get()), analysis.undefinedLabels());

                    PatternRecognitionAnalyzer.validateNoPatternAnchors(frame.getPattern().get());

                    // analyze expressions in MEASURES and DEFINE (with set of all labels passed as context)
                    for (VariableDefinition variableDefinition : frame.getVariableDefinitions()) {
                        Expression expression = variableDefinition.getExpression();
                        Type type = analyze(expression, context.getScope(), analysis.allLabels(), true);
                        resolvedLabels.put(NodeRef.of(variableDefinition.getName()), variableDefinition.getName().getCanonicalValue());

                        if (!type.equals(BOOLEAN)) {
                            throw semanticException(TYPE_MISMATCH, expression, "Expression defining a label must be boolean (actual type: %s)", type);
                        }
                    }
                    for (MeasureDefinition measureDefinition : frame.getMeasures()) {
                        Expression expression = measureDefinition.getExpression();
                        analyze(expression, context.getScope(), analysis.allLabels(), true);
                        resolvedLabels.put(NodeRef.of(measureDefinition.getName()), measureDefinition.getName().getCanonicalValue());
                    }

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
                        throw semanticException(MISSING_ROW_PATTERN, frame.getSubsets().getFirst(), "Union variable definitions require PATTERN clause");
                    }
                    if (!frame.getVariableDefinitions().isEmpty()) {
                        throw semanticException(MISSING_ROW_PATTERN, frame.getVariableDefinitions().getFirst(), "Primary pattern variable definitions require PATTERN clause");
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
                    throw semanticException(NOT_SUPPORTED, frame, "Unsupported frame type: %s", frame.getType());
                }
            }
        }

        private void analyzeFrameRangeOffset(Expression offsetValue, FrameBound.Type boundType, Context context, ResolvedWindow window, Node originalNode)
        {
            OrderBy orderBy = window.getOrderBy()
                    .orElseThrow(() -> semanticException(MISSING_ORDER_BY, originalNode, "Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY"));
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
                function = plannerContext.getMetadata().resolveOperator(operatorType, ImmutableList.of(sortKeyType, offsetValueType));
            }
            catch (TrinoException e) {
                ErrorCode errorCode = e.getErrorCode();
                if (errorCode.equals(OPERATOR_NOT_FOUND.toErrorCode())) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
                throw e;
            }
            BoundSignature signature = function.getSignature();
            Type expectedSortKeyType = signature.getArgumentTypes().getFirst();
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
        protected Type visitWindowOperation(WindowOperation node, Context context)
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

        public List<TypeSignatureProvider> getCallArgumentTypes(List<Expression> arguments, Context context)
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
                                if (context.isInLambda()) {
                                    for (LambdaArgumentDeclaration lambdaArgument : context.getFieldToLambdaArgumentDeclaration().values()) {
                                        innerExpressionAnalyzer.setExpressionType(lambdaArgument, getExpressionType(lambdaArgument));
                                    }
                                }
                                return innerExpressionAnalyzer.analyze(argument, baseScope, context.expectingLambda(types)).getTypeSignature();
                            }));
                }
                else {
                    if (isQualifiedAllFieldsReference(argument)) {
                        // to resolve `count(label.*)` correctly, we should skip the argument, like for `count(*)`
                        // process the argument but do not include it in the list
                        DereferenceExpression allRowsDereference = (DereferenceExpression) argument;
                        String label = label((Identifier) allRowsDereference.getBase());
                        if (!context.getPatternRecognitionContext().labels().contains(label)) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, allRowsDereference.getBase(), "%s is not a primary pattern variable or subset name", label);
                        }
                        labels.put(NodeRef.of(allRowsDereference), Optional.of(label));
                    }
                    else {
                        argumentTypesBuilder.add(new TypeSignatureProvider(process(argument, context).getTypeSignature()));
                    }
                }
            }

            return argumentTypesBuilder.build();
        }

        private Type analyzeMatchNumber(FunctionCall node, Context context)
        {
            if (context.isInWindow()) {
                throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, node, "MATCH_NUMBER function is not supported in window");
            }

            if (!node.getArguments().isEmpty()) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "MATCH_NUMBER pattern recognition function takes no arguments");
            }

            patternRecognitionInputs.add(new PatternInputAnalysis(node, new MatchNumberDescriptor()));

            return BIGINT;
        }

        private Type analyzeClassifier(FunctionCall node, Context context)
        {
            if (node.getArguments().size() > 1) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "CLASSIFIER pattern recognition function takes no arguments or 1 argument");
            }

            Optional<String> label = Optional.empty();
            if (node.getArguments().size() == 1) {
                Node argument = node.getArguments().getFirst();
                if (!(argument instanceof Identifier identifier)) {
                    throw semanticException(TYPE_MISMATCH, argument, "CLASSIFIER function argument should be primary pattern variable or subset name. Actual: %s", argument.getClass().getSimpleName());
                }
                label = Optional.of(label(identifier));
                if (!context.getPatternRecognitionContext().labels().contains(label.get())) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, argument, "%s is not a primary pattern variable or subset name", identifier.getValue());
                }
            }

            patternRecognitionInputs.add(new PatternInputAnalysis(
                    node,
                    new ClassifierDescriptor(label, context.getPatternRecognitionContext().navigation())));

            return VARCHAR;
        }

        private Type analyzePhysicalNavigation(FunctionCall node, Context context, String name)
        {
            validateNavigationFunctionArguments(node);

            // TODO: this should only be done at the root of a pattern recognition function call tree
            checkNoNestedAggregations(node);
            validateNavigationNesting(node);

            int offset = getNavigationOffset(node, 1);
            if (name.equals("PREV")) {
                offset = -offset;
            }

            Navigation navigation = context.getPatternRecognitionContext().navigation();
            Type type = process(
                    node.getArguments().getFirst(),
                    context.withNavigation(new Navigation(
                            navigation.anchor(),
                            navigation.mode(),
                            navigation.logicalOffset(),
                            offset)));

            // TODO: this should only be done at the root of a pattern recognition function call tree
            if (!validateLabelConsistency(node, 0).hasLabel()) {
                throw semanticException(INVALID_ARGUMENTS, node, "Pattern navigation function '%s' must contain at least one column reference or CLASSIFIER()", name);
            }

            patternNavigationFunctions.add(NodeRef.of(node));

            return type;
        }

        private Type analyzeLogicalNavigation(FunctionCall node, Context context, String name)
        {
            validateNavigationFunctionArguments(node);

            // TODO: this should only be done at the root of a pattern recognition function call tree
            checkNoNestedAggregations(node);
            validateNavigationNesting(node);

            PatternRecognitionAnalysis.NavigationAnchor anchor = switch (name) {
                case "FIRST" -> FIRST;
                case "LAST" -> LAST;
                default -> throw new IllegalStateException("Unexpected navigation anchor: " + name);
            };

            Type type = process(
                    node.getArguments().getFirst(),
                    context.withNavigation(new Navigation(
                            anchor,
                            mapProcessingMode(node.getProcessingMode()),
                            getNavigationOffset(node, 0),
                            context.getPatternRecognitionContext().navigation().physicalOffset())));

            // TODO: this should only be done at the root of a pattern recognition function call tree
            if (!validateLabelConsistency(node, 0).hasLabel()) {
                throw semanticException(INVALID_ARGUMENTS, node, "Pattern navigation function '%s' must contain at least one column reference or CLASSIFIER()", name);
            }

            patternNavigationFunctions.add(NodeRef.of(node));

            return type;
        }

        private static NavigationMode mapProcessingMode(Optional<ProcessingMode> processingMode)
        {
            return processingMode.map(mode -> switch (mode.getMode()) {
                case FINAL -> NavigationMode.FINAL;
                case RUNNING -> NavigationMode.RUNNING;
            })
            .orElse(NavigationMode.RUNNING);
        }

        private static int getNavigationOffset(FunctionCall node, int defaultOffset)
        {
            int offset = defaultOffset;
            if (node.getArguments().size() == 2) {
                offset = (int) ((LongLiteral) node.getArguments().get(1)).getParsedValue();
            }
            return offset;
        }

        private static void validatePatternRecognitionFunction(FunctionCall node)
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
        }

        private static void validateNavigationFunctionArguments(FunctionCall node)
        {
            if (node.getArguments().size() != 1 && node.getArguments().size() != 2) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s pattern recognition function requires 1 or 2 arguments", node.getName());
            }
            if (node.getArguments().size() == 2) {
                // TODO the offset argument must be effectively constant, not necessarily a number. This could be extended with the use of ConstantAnalyzer.
                if (!(node.getArguments().get(1) instanceof LongLiteral)) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s pattern recognition navigation function requires a number as the second argument", node.getName());
                }
                long offset = ((LongLiteral) node.getArguments().get(1)).getParsedValue();
                if (offset < 0) {
                    throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "%s pattern recognition navigation function requires a non-negative number as the second argument (actual: %s)", node.getName(), offset);
                }
                if (offset > Integer.MAX_VALUE) {
                    throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "The second argument of %s pattern recognition navigation function must not exceed %s (actual: %s)", node.getName(), Integer.MAX_VALUE, offset);
                }
            }
        }

        private void validateNavigationNesting(FunctionCall node)
        {
            checkArgument(isPatternNavigationFunction(node));
            String name = node.getName().getSuffix();

            // It is allowed to nest FIRST and LAST functions within PREV and NEXT functions. Only immediate nesting is supported
            List<FunctionCall> nestedNavigationFunctions = extractExpressions(ImmutableList.of(node.getArguments().getFirst()), FunctionCall.class).stream()
                    .filter(this::isPatternNavigationFunction)
                    .collect(toImmutableList());
            if (!nestedNavigationFunctions.isEmpty()) {
                if (name.equalsIgnoreCase("FIRST") || name.equalsIgnoreCase("LAST")) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nestedNavigationFunctions.getFirst(),
                            "Cannot nest %s pattern navigation function inside %s pattern navigation function", nestedNavigationFunctions.getFirst().getName(), name);
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
                if (nested != node.getArguments().getFirst()) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nested,
                            "Immediate nesting is required for pattern navigation functions");
                }
            }
        }

        private Set<String> analyzeAggregationLabels(FunctionCall node)
        {
            if (node.getArguments().isEmpty()) {
                return ImmutableSet.of();
            }

            Set<Optional<String>> argumentLabels = new HashSet<>();
            for (int i = 0; i < node.getArguments().size(); i++) {
                ArgumentLabel argumentLabel = validateLabelConsistency(node, i);
                if (argumentLabel.hasLabel()) {
                    argumentLabels.add(argumentLabel.getLabel());
                }
            }
            if (argumentLabels.size() > 1) {
                throw semanticException(INVALID_ARGUMENTS, node, "All aggregate function arguments must apply to rows matched with the same label");
            }

            return argumentLabels.stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
        }

        private ArgumentLabel validateLabelConsistency(FunctionCall node, int argumentIndex)
        {
            Set<Optional<String>> referenceLabels = extractExpressions(node.getArguments(), Expression.class).stream()
                    .map(child -> labels.get(NodeRef.of(child)))
                    .filter(Objects::nonNull)
                    .collect(toImmutableSet());

            Set<Optional<String>> classifierLabels = extractExpressions(ImmutableList.of(node.getArguments().get(argumentIndex)), FunctionCall.class).stream()
                    .filter(this::isClassifierFunction)
                    .map(functionCall -> functionCall.getArguments().stream()
                            .findFirst()
                            .map(argument -> label((Identifier) argument)))
                    .collect(toImmutableSet());

            Set<Optional<String>> allLabels = ImmutableSet.<Optional<String>>builder()
                    .addAll(referenceLabels)
                    .addAll(classifierLabels)
                    .build();

            if (allLabels.isEmpty()) {
                return ArgumentLabel.noLabel();
            }

            if (allLabels.size() > 1) {
                String name = node.getName().getSuffix();
                throw semanticException(
                        INVALID_ARGUMENTS,
                        node,
                        "All labels and classifiers inside the call to '%s' must match", name);
            }

            Optional<String> label = Iterables.getOnlyElement(allLabels);
            return label.map(ArgumentLabel::explicitLabel)
                    .orElseGet(ArgumentLabel::universalLabel);
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

        private boolean isMatchNumberFunction(FunctionCall node)
        {
            if (!isPatternRecognitionFunction(node)) {
                return false;
            }
            return node.getName().getSuffix().toUpperCase(ENGLISH).equals("MATCH_NUMBER");
        }

        private String label(Identifier identifier)
        {
            return identifier.getCanonicalValue();
        }

        private void analyzePatternAggregation(FunctionCall node, ResolvedFunction function)
        {
            checkNoNestedAggregations(node);
            checkNoNestedNavigations(node);
            Set<String> labels = analyzeAggregationLabels(node);

            List<FunctionCall> matchNumberCalls = extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(this::isMatchNumberFunction)
                    .collect(toImmutableList());

            List<FunctionCall> classifierCalls = extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(this::isClassifierFunction)
                    .collect(toImmutableList());

            patternRecognitionInputs.add(new PatternInputAnalysis(
                    node,
                    new AggregationDescriptor(
                            function,
                            node.getArguments(),
                            mapProcessingMode(node.getProcessingMode()),
                            labels,
                            matchNumberCalls,
                            classifierCalls)));
        }

        private void checkNoNestedAggregations(FunctionCall node)
        {
            extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(function -> functionResolver.isAggregationFunction(session, function.getName(), accessControl))
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
        protected Type visitAtTimeZone(AtTimeZone node, Context context)
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
        protected Type visitCurrentCatalog(CurrentCatalog node, Context context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentSchema(CurrentSchema node, Context context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentUser(CurrentUser node, Context context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitCurrentPath(CurrentPath node, Context context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitTrim(Trim node, Context context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();

            argumentTypes.add(process(node.getTrimSource(), context));
            node.getTrimCharacter().ifPresent(trimChar -> argumentTypes.add(process(trimChar, context)));
            List<Type> actualTypes = argumentTypes.build();

            String functionName = node.getSpecification().getFunctionName();
            ResolvedFunction function = plannerContext.getMetadata().resolveBuiltinFunction(functionName, fromTypes(actualTypes));

            List<Type> expectedTypes = function.getSignature().getArgumentTypes();
            checkState(expectedTypes.size() == actualTypes.size(), "wrong argument number in the resolved signature");

            Type actualTrimSourceType = actualTypes.getFirst();
            Type expectedTrimSourceType = expectedTypes.getFirst();
            coerceType(node.getTrimSource(), actualTrimSourceType, expectedTrimSourceType, "source argument of trim function");

            if (node.getTrimCharacter().isPresent()) {
                Type actualTrimCharType = actualTypes.get(1);
                Type expectedTrimCharType = expectedTypes.get(1);
                coerceType(node.getTrimCharacter().get(), actualTrimCharType, expectedTrimCharType, "trim character argument of trim function");
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            return setExpressionType(node, function.getSignature().getReturnType());
        }

        @Override
        protected Type visitFormat(Format node, Context context)
        {
            List<Type> arguments = node.getArguments().stream()
                    .map(expression -> process(expression, context))
                    .collect(toImmutableList());

            if (!(arguments.getFirst() instanceof VarcharType)) {
                throw semanticException(TYPE_MISMATCH, node.getArguments().getFirst(), "Type of first argument to format() must be VARCHAR (actual: %s)", arguments.getFirst());
            }

            for (int i = 1; i < arguments.size(); i++) {
                try {
                    plannerContext.getMetadata().resolveBuiltinFunction(FormatFunction.NAME, fromTypes(arguments.getFirst(), RowType.anonymous(arguments.subList(1, arguments.size()))));
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
        protected Type visitParameter(Parameter node, Context context)
        {
            if (isDescribe) {
                return setExpressionType(node, UNKNOWN);
            }
            if (parameters.isEmpty()) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "Query takes no parameters");
            }
            if (node.getId() >= parameters.size()) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "Invalid parameter index %s, max value is %s", node.getId(), parameters.size() - 1);
            }

            Expression providedValue = parameters.get(NodeRef.of(node));
            if (providedValue == null) {
                throw semanticException(INVALID_PARAMETER_USAGE, node, "No value provided for parameter");
            }
            Type resultType = process(providedValue, context);
            return setExpressionType(node, resultType);
        }

        @Override
        protected Type visitExtract(Extract node, Context context)
        {
            Type type = process(node.getExpression(), context);
            Extract.Field field = node.getField();

            switch (field) {
                case YEAR, MONTH:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !type.equals(INTERVAL_YEAR_MONTH)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case DAY:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !type.equals(INTERVAL_DAY_TIME)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case QUARTER, WEEK, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY, YEAR_OF_WEEK, YOW:
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case HOUR, MINUTE, SECOND:
                    if (!(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !(type instanceof TimeType) &&
                            !(type instanceof TimeWithTimeZoneType) &&
                            !type.equals(INTERVAL_DAY_TIME)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                    break;
                case TIMEZONE_MINUTE, TIMEZONE_HOUR:
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
        protected Type visitBetweenPredicate(BetweenPredicate node, Context context)
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
                addOrReplaceExpressionCoercion(node.getValue(), commonType.get());
            }
            if (!minType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(node.getMin(), commonType.get());
            }
            if (!maxType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(node.getMax(), commonType.get());
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        public Type visitTryExpression(TryExpression node, Context context)
        {
            // TRY is rewritten to lambda, and lambda is not supported in pattern recognition
            if (context.isPatternRecognition()) {
                throw semanticException(NOT_SUPPORTED, node, "TRY expression in pattern recognition context is not yet supported");
            }

            Type type = process(node.getInnerExpression(), context);
            return setExpressionType(node, type);
        }

        @Override
        public Type visitCast(Cast node, Context context)
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
            if (!value.equals(UNKNOWN)) {
                try {
                    plannerContext.getMetadata().getCoercion(value, type);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitInPredicate(InPredicate node, Context context)
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
            if (context.isPatternRecognition() && valueList instanceof SubqueryExpression) {
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
                                String label = label(qualifiedName.getOriginalParts().getFirst());
                                if (context.getPatternRecognitionContext().labels().contains(label)) {
                                    throw semanticException(NOT_SUPPORTED, dereference, "IN-PREDICATE with labeled column reference is not yet supported");
                                }
                            }
                        });

                patternRecognitionInputs.add(new PatternInputAnalysis(
                        node,
                        new ScalarInputDescriptor(Optional.empty(), context.getPatternRecognitionContext().navigation())));
            }

            if (valueList instanceof InListExpression inListExpression) {
                Type type = coerceToSingleType(context,
                        "IN value and list items",
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
        protected Type visitSubqueryExpression(SubqueryExpression node, Context context)
        {
            Type type = analyzeSubquery(node, context);

            // the implied type of a scalar subquery is that of the unique field in the single-column row
            if (type instanceof RowType && ((RowType) type).getFields().size() == 1) {
                type = type.getTypeParameters().getFirst();
            }

            setExpressionType(node, type);
            subqueries.add(NodeRef.of(node));

            if (context.isPatternRecognition()) {
                patternRecognitionInputs.add(new PatternInputAnalysis(
                        node,
                        new ScalarInputDescriptor(Optional.empty(), context.getPatternRecognitionContext().navigation())));
            }

            return type;
        }

        /**
         * @return the common supertype between the value type and subquery type
         */
        private Type analyzePredicateWithSubquery(Expression node, Type declaredValueType, SubqueryExpression subquery, Context context)
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

        private Type analyzeSubquery(SubqueryExpression node, Context context)
        {
            if (context.isInLambda()) {
                throw semanticException(NOT_SUPPORTED, node, "Lambda expression cannot contain subqueries");
            }
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node, context.getCorrelationSupport());
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getScope())
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
        protected Type visitExists(ExistsPredicate node, Context context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node, context.getCorrelationSupport());
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getScope())
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

            if (context.isPatternRecognition()) {
                patternRecognitionInputs.add(new PatternInputAnalysis(
                        node,
                        new ScalarInputDescriptor(Optional.empty(), context.getPatternRecognitionContext().navigation())));
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Context context)
        {
            quantifiedComparisons.add(NodeRef.of(node));

            Type declaredValueType = process(node.getValue(), context);
            Type comparisonType = analyzePredicateWithSubquery(node, declaredValueType, (SubqueryExpression) node.getSubquery(), context);

            switch (node.getOperator()) {
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL:
                    if (!comparisonType.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, node, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                case EQUAL, NOT_EQUAL:
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
        public Type visitFieldReference(FieldReference node, Context context)
        {
            ResolvedField field = baseScope.getField(node.getFieldIndex());
            return handleResolvedField(node, field, context);
        }

        @Override
        protected Type visitLambdaExpression(LambdaExpression node, Context context)
        {
            if (context.isPatternRecognition()) {
                throw semanticException(NOT_SUPPORTED, node, "Lambda expression in pattern recognition context is not yet supported");
            }

            verifyNoAggregateWindowOrGroupingFunctions(session, functionResolver, accessControl, node.getBody(), "Lambda expression");
            if (!context.isExpectingLambda()) {
                throw semanticException(TYPE_MISMATCH, node, "Lambda expression should always be used inside a function");
            }

            List<Type> types = context.getFunctionInputTypes();
            List<LambdaArgumentDeclaration> lambdaArguments = node.getArguments();

            if (types.size() != lambdaArguments.size()) {
                throw semanticException(INVALID_PARAMETER_USAGE, node,
                        "Expected a lambda that takes %s argument(s) but got %s", types.size(), lambdaArguments.size());
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < lambdaArguments.size(); i++) {
                LambdaArgumentDeclaration lambdaArgument = lambdaArguments.get(i);
                Type type = types.get(i);
                fields.add(Field.newUnqualified(lambdaArgument.getName().getValue(), type));
                setExpressionType(lambdaArgument, type);
            }

            Scope lambdaScope = Scope.builder()
                    .withParent(context.getScope())
                    .withRelationType(RelationId.of(node), new RelationType(fields.build()))
                    .build();

            ImmutableMap.Builder<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration = ImmutableMap.builder();
            if (context.isInLambda()) {
                fieldToLambdaArgumentDeclaration.putAll(context.getFieldToLambdaArgumentDeclaration());
            }
            for (LambdaArgumentDeclaration lambdaArgument : lambdaArguments) {
                ResolvedField resolvedField = lambdaScope.resolveField(lambdaArgument, QualifiedName.of(lambdaArgument.getName().getValue()));
                fieldToLambdaArgumentDeclaration.put(FieldId.from(resolvedField), lambdaArgument);
            }

            Type returnType = process(node.getBody(), context.inLambda(lambdaScope, fieldToLambdaArgumentDeclaration.buildOrThrow()));
            FunctionType functionType = new FunctionType(types, returnType);
            return setExpressionType(node, functionType);
        }

        @Override
        protected Type visitBindExpression(BindExpression node, Context context)
        {
            verify(context.isExpectingLambda(), "bind expression found when lambda is not expected");

            Context innerContext = context.notExpectingLambda();
            ImmutableList.Builder<Type> functionInputTypesBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                functionInputTypesBuilder.add(process(value, innerContext));
            }
            functionInputTypesBuilder.addAll(context.getFunctionInputTypes());
            List<Type> functionInputTypes = functionInputTypesBuilder.build();

            FunctionType functionType = (FunctionType) process(node.getFunction(), context.expectingLambda(functionInputTypes));

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
        protected Type visitExpression(Expression node, Context context)
        {
            throw semanticException(NOT_SUPPORTED, node, "not yet implemented: %s", node.getClass().getName());
        }

        @Override
        protected Type visitNode(Node node, Context context)
        {
            throw semanticException(NOT_SUPPORTED, node, "not yet implemented: %s", node.getClass().getName());
        }

        @Override
        public Type visitGroupingOperation(GroupingOperation node, Context context)
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
            return setExpressionType(node, BIGINT);
        }

        @Override
        public Type visitJsonExists(JsonExists node, Context context)
        {
            List<Type> pathInvocationArgumentTypes = analyzeJsonPathInvocation("JSON_EXISTS", node, node.getJsonPathInvocation(), context);

            // pass remaining information in the node : error behavior
            List<Type> argumentTypes = ImmutableList.<Type>builder()
                    .addAll(pathInvocationArgumentTypes)
                    .add(TINYINT) // enum encoded as integer value
                    .build();

            // resolve function
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(JSON_EXISTS_FUNCTION_NAME, fromTypes(argumentTypes));
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);
            Type type = function.getSignature().getReturnType();

            return setExpressionType(node, type);
        }

        @Override
        public Type visitJsonValue(JsonValue node, Context context)
        {
            List<Type> pathInvocationArgumentTypes = analyzeJsonPathInvocation("JSON_VALUE", node, node.getJsonPathInvocation(), context);
            Type returnedType = analyzeJsonValueExpression(
                    node,
                    jsonPathAnalyses.get(NodeRef.of(node)),
                    node.getReturnedType(),
                    pathInvocationArgumentTypes,
                    node.getEmptyBehavior(),
                    node.getEmptyDefault(),
                    Optional.of(node.getErrorBehavior()),
                    node.getErrorDefault(),
                    context);
            return setExpressionType(node, returnedType);
        }

        private Type analyzeJsonValueExpression(
                Node node,
                JsonPathAnalysis pathAnalysis,
                Optional<DataType> declaredReturnedType,
                List<Type> pathInvocationArgumentTypes,
                JsonValue.EmptyOrErrorBehavior emptyBehavior,
                Optional<Expression> declaredEmptyDefault,
                Optional<JsonValue.EmptyOrErrorBehavior> errorBehavior,
                Optional<Expression> declaredErrorDefault,
                Context context)
        {
            // validate returned type
            Type returnedType = VARCHAR; // default
            if (declaredReturnedType.isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeSignature(declaredReturnedType.get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", declaredReturnedType.get());
                }
            }

            if (!isCharacterStringType(returnedType) &&
                    !isNumericType(returnedType) &&
                    !returnedType.equals(BOOLEAN) &&
                    !isDateTimeType(returnedType) ||
                    returnedType.equals(INTERVAL_DAY_TIME) ||
                    returnedType.equals(INTERVAL_YEAR_MONTH)) {
                throw semanticException(TYPE_MISMATCH, node, "Invalid return type of function JSON_VALUE: %s", declaredReturnedType.get());
            }

            Type resultType = pathAnalysis.getType(pathAnalysis.getPath());
            if (resultType != null && !resultType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(resultType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Return type of JSON path: %s incompatible with return type of function JSON_VALUE: %s", resultType, returnedType);
                }
            }

            // validate default values for empty and error behavior
            if (declaredEmptyDefault.isPresent()) {
                Expression emptyDefault = declaredEmptyDefault.get();
                if (emptyBehavior != DEFAULT) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, emptyDefault, "Default value specified for %s ON EMPTY behavior", emptyBehavior);
                }
                Type type = process(emptyDefault, context);
                // this would normally be done after function resolution, but we know that the default expression is always coerced to the returnedType
                coerceType(emptyDefault, type, returnedType, "Function JSON_VALUE default ON EMPTY result");
            }

            if (declaredErrorDefault.isPresent()) {
                Expression errorDefault = declaredErrorDefault.get();
                if (errorBehavior.isEmpty()) {
                    throw new IllegalStateException("error default specified without error behavior specified");
                }
                if (errorBehavior.orElseThrow() != DEFAULT) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, errorDefault, "Default value specified for %s ON ERROR behavior", errorBehavior.orElseThrow());
                }
                Type type = process(errorDefault, context);
                // this would normally be done after function resolution, but we know that the default expression is always coerced to the returnedType
                coerceType(errorDefault, type, returnedType, "Function JSON_VALUE default ON ERROR result");
            }

            // pass remaining information in the node : empty behavior, empty default, error behavior, error default
            List<Type> argumentTypes = ImmutableList.<Type>builder()
                    .addAll(pathInvocationArgumentTypes)
                    .add(TINYINT) // empty behavior: enum encoded as integer value
                    .add(returnedType) // empty default
                    .add(TINYINT) // error behavior: enum encoded as integer value
                    .add(returnedType) // error default
                    .build();

            // resolve function
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(JSON_VALUE_FUNCTION_NAME, fromTypes(argumentTypes));
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            return function.getSignature().getReturnType();
        }

        @Override
        public Type visitJsonQuery(JsonQuery node, Context context)
        {
            List<Type> pathInvocationArgumentTypes = analyzeJsonPathInvocation("JSON_QUERY", node, node.getJsonPathInvocation(), context);
            Type returnedType = analyzeJsonQueryExpression(
                    node,
                    node.getWrapperBehavior(),
                    node.getQuotesBehavior(),
                    pathInvocationArgumentTypes,
                    node.getReturnedType(),
                    node.getOutputFormat());
            return setExpressionType(node, returnedType);
        }

        private Type analyzeJsonQueryExpression(
                Node node,
                JsonQuery.ArrayWrapperBehavior wrapperBehavior,
                Optional<JsonQuery.QuotesBehavior> quotesBehavior,
                List<Type> pathInvocationArgumentTypes,
                Optional<DataType> declaredReturnedType,
                Optional<JsonFormat> declaredOutputFormat)
        {
            // wrapper behavior, empty behavior and error behavior will be passed as arguments to function
            // quotes behavior is handled by the corresponding output function
            List<Type> argumentTypes = ImmutableList.<Type>builder()
                    .addAll(pathInvocationArgumentTypes)
                    .add(TINYINT) // wrapper behavior: enum encoded as integer value
                    .add(TINYINT) // empty behavior: enum encoded as integer value
                    .add(TINYINT) // error behavior: enum encoded as integer value
                    .build();

            // validate wrapper and quotes behavior
            if ((wrapperBehavior == CONDITIONAL || wrapperBehavior == UNCONDITIONAL) && quotesBehavior.isPresent()) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s QUOTES behavior specified with WITH %s ARRAY WRAPPER behavior", quotesBehavior.get(), wrapperBehavior);
            }

            // resolve function
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(JSON_QUERY_FUNCTION_NAME, fromTypes(argumentTypes));
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            // analyze returned type and format
            Type returnedType = VARCHAR; // default
            if (declaredReturnedType.isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeSignature(declaredReturnedType.get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", declaredReturnedType.get());
                }
            }
            JsonFormat outputFormat = declaredOutputFormat.orElse(JsonFormat.JSON); // default

            // resolve function to format output
            ResolvedFunction outputFunction = getOutputFunction(returnedType, outputFormat, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            // cast the output value to the declared returned type if necessary
            Type outputType = outputFunction.getSignature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", outputType, returnedType);
                }
            }

            return returnedType;
        }

        private List<Type> analyzeJsonPathInvocation(String functionName, Node node, JsonPathInvocation jsonPathInvocation, Context context)
        {
            jsonPathInvocation.getPathName().ifPresent(pathName -> {
                if (!(node instanceof JsonTable)) {
                    throw semanticException(INVALID_PATH, pathName, "JSON path name is not allowed in %s function", functionName);
                }
            });

            // ANALYZE THE CONTEXT ITEM
            // analyze context item type
            Expression inputExpression = jsonPathInvocation.getInputExpression();
            Type inputType = process(inputExpression, context);

            // resolve function to read the context item as JSON
            JsonFormat inputFormat = jsonPathInvocation.getInputFormat();
            ResolvedFunction inputFunction = getInputFunction(inputType, inputFormat, inputExpression);
            Type expectedType = inputFunction.getSignature().getArgumentType(0);
            coerceType(inputExpression, inputType, expectedType, format("%s function input argument", functionName));
            jsonInputFunctions.put(NodeRef.of(inputExpression), inputFunction);

            // ANALYZE JSON PATH PARAMETERS
            // TODO verify parameter count? Is there a limit on Row size?

            ImmutableMap.Builder<String, Type> types = ImmutableMap.builder(); // record parameter types for JSON path analysis
            Set<String> uniqueNames = new HashSet<>(); // validate parameter names

            // this node will be translated into a FunctionCall, and all the information it carries will be passed as arguments to the FunctionCall.
            // all JSON path parameters are wrapped in a Row, and constitute a single FunctionCall argument.
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();

            List<JsonPathParameter> pathParameters = jsonPathInvocation.getPathParameters();
            for (JsonPathParameter pathParameter : pathParameters) {
                Expression parameter = pathParameter.getParameter();
                String parameterName = pathParameter.getName().getCanonicalValue();
                Optional<JsonFormat> parameterFormat = pathParameter.getFormat();

                // type of the parameter passed to the JSON path:
                // - parameters of types numeric, string, boolean, date,... are passed as-is
                // - parameters with explicit or implicit FORMAT, are converted to JSON (type JSON_2016)
                // - all other parameters are cast to VARCHAR
                Type passedType;

                if (!uniqueNames.add(parameterName)) {
                    throw semanticException(DUPLICATE_PARAMETER_NAME, pathParameter.getName(), "%s JSON path parameter is specified more than once", parameterName);
                }

                if (parameter instanceof LambdaExpression || parameter instanceof BindExpression) {
                    throw semanticException(NOT_SUPPORTED, parameter, "%s is not supported as JSON path parameter", parameter.getClass().getSimpleName());
                }
                // if the input expression is a JSON-returning function, there should be an explicit or implicit input format (spec p.817)
                // JSON-returning functions are: JSON_OBJECT, JSON_OBJECTAGG, JSON_ARRAY, JSON_ARRAYAGG and JSON_QUERY
                if ((parameter instanceof JsonQuery ||
                        parameter instanceof JsonObject ||
                        parameter instanceof JsonArray) && // TODO add JSON_OBJECTAGG, JSON_ARRAYAGG when supported
                        parameterFormat.isEmpty()) {
                    parameterFormat = Optional.of(JsonFormat.JSON);
                }

                Type parameterType = process(parameter, context);
                if (parameterFormat.isPresent()) {
                    // resolve function to read the parameter as JSON
                    ResolvedFunction parameterInputFunction = getInputFunction(parameterType, parameterFormat.get(), parameter);
                    Type expectedParameterType = parameterInputFunction.getSignature().getArgumentType(0);
                    coerceType(parameter, parameterType, expectedParameterType, format("%s function JSON path parameter", functionName));
                    jsonInputFunctions.put(NodeRef.of(parameter), parameterInputFunction);
                    passedType = JSON_2016;
                }
                else {
                    if (isStringType(parameterType)) {
                        if (!isCharacterStringType(parameterType)) {
                            throw semanticException(NOT_SUPPORTED, parameter, "Unsupported type of JSON path parameter: %s", parameterType.getDisplayName());
                        }
                        passedType = parameterType;
                    }
                    else if (isNumericType(parameterType) || parameterType.equals(BOOLEAN)) {
                        passedType = parameterType;
                    }
                    else if (isDateTimeType(parameterType) && !parameterType.equals(INTERVAL_DAY_TIME) && !parameterType.equals(INTERVAL_YEAR_MONTH)) {
                        passedType = parameterType;
                    }
                    else {
                        try {
                            plannerContext.getMetadata().getCoercion(parameterType, VARCHAR);
                        }
                        catch (OperatorNotFoundException e) {
                            throw semanticException(NOT_SUPPORTED, node, "Unsupported type of JSON path parameter: %s", parameterType.getDisplayName());
                        }
                        addOrReplaceExpressionCoercion(parameter, VARCHAR);
                        passedType = VARCHAR;
                    }
                }

                types.put(parameterName, passedType);
                fields.add(new RowType.Field(Optional.of(parameterName), passedType));
            }

            Type parametersRowType = JSON_NO_PARAMETERS_ROW_TYPE;
            if (!pathParameters.isEmpty()) {
                parametersRowType = RowType.from(fields.build());
            }

            // ANALYZE JSON PATH
            Map<String, Type> typesMap = types.buildOrThrow();
            JsonPathAnalysis pathAnalysis = new JsonPathAnalyzer(
                    plannerContext.getMetadata(),
                    createConstantAnalyzer(plannerContext, accessControl, session, ExpressionAnalyzer.this.parameters, WarningCollector.NOOP))
                    .analyzeJsonPath(jsonPathInvocation.getJsonPath(), typesMap);
            jsonPathAnalyses.put(NodeRef.of(node), pathAnalysis);

            return ImmutableList.of(
                    JSON_2016, // input expression
                    plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)), // parsed JSON path representation
                    parametersRowType); // passed parameters
        }

        private ResolvedFunction getInputFunction(Type type, JsonFormat format, Node node)
        {
            String name = switch (format) {
                case JSON -> {
                    if (UNKNOWN.equals(type) || isCharacterStringType(type)) {
                        yield VARCHAR_TO_JSON;
                    }
                    if (isStringType(type)) {
                        yield VARBINARY_TO_JSON;
                    }
                    throw semanticException(TYPE_MISMATCH, node, "Cannot read input of type %s as JSON using formatting %s", type, format);
                }
                case UTF8 -> VARBINARY_UTF8_TO_JSON;
                case UTF16 -> VARBINARY_UTF16_TO_JSON;
                case UTF32 -> VARBINARY_UTF32_TO_JSON;
            };

            try {
                return plannerContext.getMetadata().resolveBuiltinFunction(name, fromTypes(type, BOOLEAN));
            }
            catch (TrinoException e) {
                throw new TrinoException(TYPE_MISMATCH, extractLocation(node), format("Cannot read input of type %s as JSON using formatting %s", type, format), e);
            }
        }

        private ResolvedFunction getOutputFunction(Type type, JsonFormat format, Node node)
        {
            String name = switch (format) {
                case JSON -> {
                    if (isCharacterStringType(type)) {
                        yield JSON_TO_VARCHAR;
                    }
                    if (isStringType(type)) {
                        yield JSON_TO_VARBINARY;
                    }
                    throw semanticException(TYPE_MISMATCH, node, "Cannot output JSON value as %s using formatting %s", type, format);
                }
                case UTF8 -> {
                    if (!VARBINARY.equals(type)) {
                        throw semanticException(TYPE_MISMATCH, node, "Cannot output JSON value as %s using formatting %s", type, format);
                    }
                    yield JSON_TO_VARBINARY_UTF8;
                }
                case UTF16 -> {
                    if (!VARBINARY.equals(type)) {
                        throw semanticException(TYPE_MISMATCH, node, "Cannot output JSON value as %s using formatting %s", type, format);
                    }
                    yield JSON_TO_VARBINARY_UTF16;
                }
                case UTF32 -> {
                    if (!VARBINARY.equals(type)) {
                        throw semanticException(TYPE_MISMATCH, node, "Cannot output JSON value as %s using formatting %s", type, format);
                    }
                    yield JSON_TO_VARBINARY_UTF32;
                }
            };

            try {
                return plannerContext.getMetadata().resolveBuiltinFunction(name, fromTypes(JSON_2016, TINYINT, BOOLEAN));
            }
            catch (TrinoException e) {
                throw new TrinoException(TYPE_MISMATCH, extractLocation(node), format("Cannot output JSON value as %s using formatting %s", type, format), e);
            }
        }

        @Override
        protected Type visitJsonObject(JsonObject node, Context context)
        {
            // TODO verify parameter count? Is there a limit on Row size?

            ImmutableList.Builder<RowType.Field> keyFields = ImmutableList.builder();
            ImmutableList.Builder<RowType.Field> valueFields = ImmutableList.builder();

            for (JsonObjectMember member : node.getMembers()) {
                Expression key = member.getKey();
                Expression value = member.getValue();
                Optional<JsonFormat> format = member.getFormat();

                Type keyType = process(key, context);
                if (!isCharacterStringType(keyType)) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, key, "Invalid type of JSON object key: %s", keyType.getDisplayName());
                }
                keyFields.add(new RowType.Field(Optional.empty(), keyType));

                if (value instanceof LambdaExpression || value instanceof BindExpression) {
                    throw semanticException(NOT_SUPPORTED, value, "%s is not supported as JSON object value", value.getClass().getSimpleName());
                }

                // types accepted for values of a JSON object:
                // - values of types numeric, string, and boolean are passed as-is
                // - values with explicit or implicit FORMAT, are converted to JSON (type JSON_2016)
                // - all other values are cast to VARCHAR

                // if the value expression is a JSON-returning function, there should be an explicit or implicit input format (spec p.817)
                // JSON-returning functions are: JSON_OBJECT, JSON_OBJECTAGG, JSON_ARRAY, JSON_ARRAYAGG and JSON_QUERY
                if ((value instanceof JsonQuery ||
                        value instanceof JsonObject ||
                        value instanceof JsonArray) && // TODO add JSON_OBJECTAGG, JSON_ARRAYAGG when supported
                        format.isEmpty()) {
                    format = Optional.of(JsonFormat.JSON);
                }

                Type valueType = process(value, context);

                if (format.isPresent()) {
                    // in case when there is an input expression with FORMAT option, the only supported behavior
                    // for the JSON_OBJECT function is WITHOUT UNIQUE KEYS. This is because the functions used for
                    // converting input to JSON only support this option.
                    if (node.isUniqueKeys()) {
                        throw semanticException(NOT_SUPPORTED, node, "WITH UNIQUE KEYS behavior is not supported for JSON_OBJECT function when input expression has FORMAT");
                    }
                    // resolve function to read the value as JSON
                    ResolvedFunction inputFunction = getInputFunction(valueType, format.get(), value);
                    Type expectedValueType = inputFunction.getSignature().getArgumentType(0);
                    coerceType(value, valueType, expectedValueType, "value passed to JSON_OBJECT function");
                    jsonInputFunctions.put(NodeRef.of(value), inputFunction);
                    valueType = JSON_2016;
                }
                else {
                    if (isStringType(valueType)) {
                        if (!isCharacterStringType(valueType)) {
                            throw semanticException(NOT_SUPPORTED, value, "Unsupported type of value passed to JSON_OBJECT function: %s", valueType.getDisplayName());
                        }
                    }

                    if (!isStringType(valueType) && !isNumericType(valueType) && !valueType.equals(BOOLEAN)) {
                        try {
                            plannerContext.getMetadata().getCoercion(valueType, VARCHAR);
                        }
                        catch (OperatorNotFoundException e) {
                            throw semanticException(NOT_SUPPORTED, node, "Unsupported type of value passed to JSON_OBJECT function: %s", valueType.getDisplayName());
                        }
                        addOrReplaceExpressionCoercion(value, VARCHAR);
                        valueType = VARCHAR;
                    }
                }

                valueFields.add(new RowType.Field(Optional.empty(), valueType));
            }

            RowType keysRowType = JSON_NO_PARAMETERS_ROW_TYPE;
            RowType valuesRowType = JSON_NO_PARAMETERS_ROW_TYPE;
            if (!node.getMembers().isEmpty()) {
                keysRowType = RowType.from(keyFields.build());
                valuesRowType = RowType.from(valueFields.build());
            }

            // resolve function
            List<Type> argumentTypes = ImmutableList.of(keysRowType, valuesRowType, BOOLEAN, BOOLEAN);
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(JSON_OBJECT_FUNCTION_NAME, fromTypes(argumentTypes));
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            // analyze returned type and format
            Type returnedType = VARCHAR; // default
            if (node.getReturnedType().isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeSignature(node.getReturnedType().get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", node.getReturnedType().get());
                }
            }
            JsonFormat outputFormat = node.getOutputFormat().orElse(JsonFormat.JSON); // default

            // resolve function to format output
            ResolvedFunction outputFunction = getOutputFunction(returnedType, outputFormat, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            // cast the output value to the declared returned type if necessary
            Type outputType = outputFunction.getSignature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot return type %s from JSON_OBJECT function", returnedType);
                }
            }

            return setExpressionType(node, returnedType);
        }

        @Override
        protected Type visitJsonArray(JsonArray node, Context context)
        {
            // TODO verify parameter count? Is there a limit on Row size?

            ImmutableList.Builder<RowType.Field> elementFields = ImmutableList.builder();

            for (JsonArrayElement arrayElement : node.getElements()) {
                Expression element = arrayElement.getValue();
                Optional<JsonFormat> format = arrayElement.getFormat();

                if (element instanceof LambdaExpression || element instanceof BindExpression) {
                    throw semanticException(NOT_SUPPORTED, element, "%s is not supported as JSON array element", element.getClass().getSimpleName());
                }

                // types accepted for elements of a JSON array:
                // - values of types numeric, string, and boolean are passed as-is
                // - values with explicit or implicit FORMAT, are converted to JSON (type JSON_2016)
                // - all other values are cast to VARCHAR

                // if the value expression is a JSON-returning function, there should be an explicit or implicit input format (spec p.817)
                // JSON-returning functions are: JSON_OBJECT, JSON_OBJECTAGG, JSON_ARRAY, JSON_ARRAYAGG and JSON_QUERY
                if ((element instanceof JsonQuery ||
                        element instanceof JsonObject ||
                        element instanceof JsonArray) && // TODO add JSON_OBJECTAGG, JSON_ARRAYAGG when supported
                        format.isEmpty()) {
                    format = Optional.of(JsonFormat.JSON);
                }

                Type elementType = process(element, context);

                if (format.isPresent()) {
                    // resolve function to read the value as JSON
                    ResolvedFunction inputFunction = getInputFunction(elementType, format.get(), element);
                    Type expectedElementType = inputFunction.getSignature().getArgumentType(0);
                    coerceType(element, elementType, expectedElementType, "value passed to JSON_ARRAY function");
                    jsonInputFunctions.put(NodeRef.of(element), inputFunction);
                    elementType = JSON_2016;
                }
                else {
                    if (isStringType(elementType)) {
                        if (!isCharacterStringType(elementType)) {
                            throw semanticException(NOT_SUPPORTED, element, "Unsupported type of value passed to JSON_ARRAY function: %s", elementType.getDisplayName());
                        }
                    }

                    if (!isStringType(elementType) && !isNumericType(elementType) && !elementType.equals(BOOLEAN)) {
                        try {
                            plannerContext.getMetadata().getCoercion(elementType, VARCHAR);
                        }
                        catch (OperatorNotFoundException e) {
                            throw semanticException(NOT_SUPPORTED, node, "Unsupported type of value passed to JSON_ARRAY function: %s", elementType.getDisplayName());
                        }
                        addOrReplaceExpressionCoercion(element, VARCHAR);
                        elementType = VARCHAR;
                    }
                }

                elementFields.add(new RowType.Field(Optional.empty(), elementType));
            }

            RowType elementsRowType = JSON_NO_PARAMETERS_ROW_TYPE;
            if (!node.getElements().isEmpty()) {
                elementsRowType = RowType.from(elementFields.build());
            }

            // resolve function
            List<Type> argumentTypes = ImmutableList.of(elementsRowType, BOOLEAN);
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(JSON_ARRAY_FUNCTION_NAME, fromTypes(argumentTypes));
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            // analyze returned type and format
            Type returnedType = VARCHAR; // default
            if (node.getReturnedType().isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeSignature(node.getReturnedType().get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", node.getReturnedType().get());
                }
            }
            JsonFormat outputFormat = node.getOutputFormat().orElse(JsonFormat.JSON); // default

            // resolve function to format output
            ResolvedFunction outputFunction = getOutputFunction(returnedType, outputFormat, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            // cast the output value to the declared returned type if necessary
            Type outputType = outputFunction.getSignature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot return type %s from JSON_ARRAY function", returnedType);
                }
            }

            return setExpressionType(node, returnedType);
        }

        private Type getOperator(Context context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            BoundSignature operatorSignature;
            try {
                operatorSignature = plannerContext.getMetadata().resolveOperator(operatorType, argumentTypes.build()).getSignature();
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
                addOrReplaceExpressionCoercion(expression, expectedType);
            }
        }

        private void coerceType(Context context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private Type coerceToSingleType(Context context, Node node, String message, Expression first, Expression second)
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
                    addOrReplaceExpressionCoercion(first, superType);
                }
                if (!secondType.equals(superType)) {
                    addOrReplaceExpressionCoercion(second, superType);
                }
                return superType;
            }

            throw semanticException(TYPE_MISMATCH, node, "%s: %s vs %s", message, firstType, secondType);
        }

        private Type coerceToSingleType(Context context, String description, List<Expression> expressions)
        {
            // determine super type
            Type superType = UNKNOWN;

            // Use LinkedHashMultimap to preserve order in which expressions are analyzed within IN list
            Multimap<Type, NodeRef<Expression>> typeExpressions = LinkedHashMultimap.create();
            for (Expression expression : expressions) {
                // We need to wrap as NodeRef since LinkedHashMultimap does not allow duplicated values
                Type type = process(expression, context);
                typeExpressions.put(type, NodeRef.of(expression));
            }

            Set<Type> types = typeExpressions.keySet();

            for (Type type : types) {
                Optional<Type> newSuperType = typeCoercion.getCommonSuperType(superType, type);
                if (newSuperType.isEmpty()) {
                    throw semanticException(TYPE_MISMATCH, Iterables.get(typeExpressions.get(type), 0).getNode(),
                            "%s must be the same type or coercible to a common type. Cannot find common type between %s and %s, all types (without duplicates): %s",
                            description,
                            superType,
                            type,
                            typeExpressions.keySet());
                }
                superType = newSuperType.get();
            }

            // verify all expressions can be coerced to the superType
            for (Type type : types) {
                Collection<NodeRef<Expression>> coercionCandidates = typeExpressions.get(type);

                if (!type.equals(superType)) {
                    if (!typeCoercion.canCoerce(type, superType)) {
                        throw semanticException(TYPE_MISMATCH, Iterables.get(coercionCandidates, 0).getNode(),
                                "%s must be the same type or coercible to a common type. Cannot find common type between %s and %s, all types (without duplicates): %s",
                                description,
                                superType,
                                type,
                                typeExpressions.keySet());
                    }
                    addOrReplaceExpressionsCoercion(coercionCandidates, superType);
                }
            }

            return superType;
        }

        private void addOrReplaceExpressionCoercion(Expression expression, Type superType)
        {
            addOrReplaceExpressionsCoercion(ImmutableList.of(NodeRef.of(expression)), superType);
        }

        private void addOrReplaceExpressionsCoercion(Collection<NodeRef<Expression>> expressions, Type superType)
        {
            expressions.forEach(expression -> expressionCoercions.put(expression, superType));
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

        private final Optional<PatternRecognitionContext> patternRecognitionContext;

        private final CorrelationSupport correlationSupport;

        private final boolean inWindow;

        private Context(
                Scope scope,
                List<Type> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration,
                Optional<PatternRecognitionContext> patternRecognitionContext,
                CorrelationSupport correlationSupport,
                boolean inWindow)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
            this.patternRecognitionContext = requireNonNull(patternRecognitionContext, "patternRecognitionContext is null");
            this.correlationSupport = requireNonNull(correlationSupport, "correlationSupport is null");
            this.inWindow = inWindow;
        }

        public static Context notInLambda(Scope scope, CorrelationSupport correlationSupport)
        {
            return new Context(scope, null, null, Optional.empty(), correlationSupport, false);
        }

        public Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"), patternRecognitionContext, correlationSupport, inWindow);
        }

        public static Context inWindow(Scope scope, CorrelationSupport correlationSupport)
        {
            return new Context(scope, null, null, Optional.empty(), correlationSupport, true);
        }

        public Context inWindow()
        {
            return new Context(scope, functionInputTypes, fieldToLambdaArgumentDeclaration, patternRecognitionContext, correlationSupport, true);
        }

        public Context expectingLambda(List<Type> functionInputTypes)
        {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration, patternRecognitionContext, correlationSupport, inWindow);
        }

        public Context notExpectingLambda()
        {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration, patternRecognitionContext, correlationSupport, inWindow);
        }

        public static Context patternRecognition(Scope scope, Set<String> labels, boolean inWindow)
        {
            return new Context(scope, null, null, Optional.of(new PatternRecognitionContext(labels, Navigation.DEFAULT)), CorrelationSupport.DISALLOWED, inWindow);
        }

        public Context withNavigation(Navigation navigation)
        {
            PatternRecognitionContext patternRecognitionContext = new PatternRecognitionContext(this.patternRecognitionContext.get().labels, navigation);
            return new Context(
                    scope,
                    functionInputTypes,
                    fieldToLambdaArgumentDeclaration,
                    Optional.of(patternRecognitionContext),
                    correlationSupport,
                    inWindow);
        }

        Scope getScope()
        {
            return scope;
        }

        public boolean isInLambda()
        {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isInWindow()
        {
            return inWindow;
        }

        public boolean isExpectingLambda()
        {
            return functionInputTypes != null;
        }

        public boolean isPatternRecognition()
        {
            return patternRecognitionContext.isPresent();
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

        public PatternRecognitionContext getPatternRecognitionContext()
        {
            return patternRecognitionContext.get();
        }

        public CorrelationSupport getCorrelationSupport()
        {
            return correlationSupport;
        }

        record PatternRecognitionContext(Set<String> labels, Navigation navigation) {}
    }

    public static boolean isPatternRecognitionFunction(FunctionCall node)
    {
        QualifiedName qualifiedName = node.getName();
        if (qualifiedName.getParts().size() > 1) {
            return false;
        }
        Identifier identifier = qualifiedName.getOriginalParts().getFirst();
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
        analyzer.analyze(expression, scope, labels, false);

        updateAnalysis(analysis, analyzer, session, accessControl);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
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
        Analysis analysis = new Analysis(null, parameters, queryType);
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, types, warningCollector);
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
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ParametersTypeAndAnalysis analyzeJsonPathInvocation(
            JsonTable node,
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        RowType parametersRowType = analyzer.analyzeJsonPathInvocation(node, scope, correlationSupport);
        updateAnalysis(analysis, analyzer, session, accessControl);
        return new ParametersTypeAndAnalysis(
                parametersRowType,
                new ExpressionAnalysis(
                        analyzer.getExpressionTypes(),
                        analyzer.getExpressionCoercions(),
                        analyzer.getSubqueryInPredicates(),
                        analyzer.getSubqueries(),
                        analyzer.getExistsSubqueries(),
                        analyzer.getColumnReferences(),
                        analyzer.getQuantifiedComparisons(),
                        analyzer.getWindowFunctions()));
    }

    public record ParametersTypeAndAnalysis(RowType parametersType, ExpressionAnalysis expressionAnalysis) {}

    public static TypeAndAnalysis analyzeJsonValueExpression(
            ValueColumn column,
            JsonPathAnalysis pathAnalysis,
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        Type type = analyzer.analyzeJsonValueExpression(column, pathAnalysis, scope, correlationSupport);
        updateAnalysis(analysis, analyzer, session, accessControl);
        return new TypeAndAnalysis(type, new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions()));
    }

    public static Type analyzeJsonQueryExpression(
            QueryColumn column,
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            WarningCollector warningCollector)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, TypeProvider.empty(), warningCollector);
        Type type = analyzer.analyzeJsonQueryExpression(column, scope);
        updateAnalysis(analysis, analyzer, session, accessControl);
        return type;
    }

    public static void analyzeExpressionWithoutSubqueries(
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            Expression expression,
            ErrorCodeSupplier errorCode,
            String message,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(
                plannerContext,
                accessControl,
                (node, ignored) -> {
                    throw semanticException(errorCode, node, "%s", message);
                },
                session,
                TypeProvider.empty(),
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe(),
                analysis::getType,
                analysis::getWindow);
        analyzer.analyze(expression, scope, correlationSupport);

        updateAnalysis(analysis, analyzer, session, accessControl);
        analysis.addExpressionFields(expression, analyzer.getSourceFields());
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
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    private static void updateAnalysis(Analysis analysis, ExpressionAnalyzer analyzer, Session session, AccessControl accessControl)
    {
        analysis.addTypes(analyzer.getExpressionTypes());
        analysis.addCoercions(
                analyzer.getExpressionCoercions(),
                analyzer.getSortKeyCoercionsForFrameBoundCalculation(),
                analyzer.getSortKeyCoercionsForFrameBoundComparison());
        analysis.addFrameBoundCalculations(analyzer.getFrameBoundCalculations());
        analyzer.getResolvedFunctions().forEach((key, value) -> analysis.addResolvedFunction(key.getNode(), value, session.getUser()));
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());
        analysis.addLabels(analyzer.getLabels());
        analysis.addPatternRecognitionInputs(analyzer.getPatternRecognitionInputs());
        analysis.addPatternNavigationFunctions(analyzer.getPatternNavigationFunctions());
        analysis.setRanges(analyzer.getRanges());
        analysis.setUndefinedLabels(analyzer.getUndefinedLabels());
        analysis.addResolvedLabels(analyzer.getResolvedLabels());
        analysis.addSubsetLabels(analyzer.getSubsetLabels());
        analysis.setMeasureDefinitions(analyzer.getMeasureDefinitions());
        analysis.setJsonPathAnalyses(analyzer.getJsonPathAnalyses());
        analysis.setJsonInputFunctions(analyzer.getJsonInputFunctions());
        analysis.setJsonOutputFunctions(analyzer.getJsonOutputFunctions());
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
                node -> semanticException(errorCode, node, "%s", message),
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

    public static boolean isStringType(Type type)
    {
        return isCharacterStringType(type) || VARBINARY.equals(type);
    }

    public static boolean isCharacterStringType(Type type)
    {
        return type instanceof VarcharType || type instanceof CharType;
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

    public record TypeAndAnalysis(Type type, ExpressionAnalysis analysis) {}
}
