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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogFunctionMetadata;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.LanguageFunctionAnalysisException;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Collation;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis.OperandAndPredicate;
import io.trino.sql.analyzer.Analysis.PredicateCoercions;
import io.trino.sql.analyzer.Analysis.Range;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.CollationSemantics.Collated;
import io.trino.sql.analyzer.CollationSemantics.CollationConflictException;
import io.trino.sql.analyzer.CollationSemantics.CollationShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.ArrayShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.Leaf;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.MapShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.RowShape;
import io.trino.sql.analyzer.CollationSemantics.Derivation;
import io.trino.sql.analyzer.JsonPathAnalyzer.JsonPathAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.AggregationDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ClassifierDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.MatchNumberDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.Navigation;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationMode;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.PatternInputAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ScalarInputDescriptor;
import io.trino.sql.ir.Constant;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.ArrayWildcardSubscript;
import io.trino.sql.tree.AtLocal;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.BooleanTestPredicate;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.CollateExpression;
import io.trino.sql.tree.CollationFor;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.CompositeIntervalQualifier;
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
import io.trino.sql.tree.DistinctFromPredicate;
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
import io.trino.sql.tree.IntervalDataType;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IntervalQualifier;
import io.trino.sql.tree.IntervalValueExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonConstructor;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonPathParameter.JsonFormat;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonSerialize;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MatchPredicate;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.MethodCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.OverlapsPredicate;
import io.trino.sql.tree.Overlay;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicate;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonPredicate;
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.StaticMethodCall;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.UniquePredicate;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.SqlJsonPathType;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.LanguageFunctionManager.isTrinoSqlLanguageFunction;
import static io.trino.operator.scalar.FormatFunction.FORMAT_FUNCTION_NAME;
import static io.trino.operator.scalar.StringFunctions.OVERLAY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonArrayFunction.JSON_ARRAY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonExistsFunction.JSON_EXISTS_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonInputFunctions.JSON_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF16_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF32_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARBINARY_UTF8_TO_JSON;
import static io.trino.operator.scalar.json.JsonInputFunctions.VARCHAR_TO_JSON;
import static io.trino.operator.scalar.json.JsonObjectFunction.JSON_OBJECT_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_JSON_OUTPUT;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF16;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF32;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARBINARY_UTF8;
import static io.trino.operator.scalar.json.JsonOutputFunctions.JSON_TO_VARCHAR;
import static io.trino.operator.scalar.json.JsonQueryFunction.JSON_QUERY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonValueFunction.JSON_VALUE_FUNCTION_NAME;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLLATION_MISMATCH;
import static io.trino.spi.StandardErrorCode.COLLATION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.DUPLICATE_PARAMETER_NAME;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_DEFAULT_COLUMN_VALUE;
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
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_SUBQUERY;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static io.trino.sql.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.FIRST;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.LAST;
import static io.trino.sql.analyzer.SemanticExceptions.invalidReferenceException;
import static io.trino.sql.analyzer.SemanticExceptions.missingAttributeException;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.inferIntervalFractionalPrecision;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.inferIntervalLeadingPrecision;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.intervalQualifierHasInferableFractionalPrecision;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.intervalQualifierHasPrecision;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toTypeDescriptor;
import static io.trino.sql.ir.IrExpressions.cast;
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
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static io.trino.type.collation.Collations.hasNonDefaultCollation;
import static io.trino.type.collation.Collations.withoutCollations;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ExpressionAnalyzer
{
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    public static final RowType JSON_NO_PARAMETERS_ROW_TYPE = RowType.anonymous(ImmutableList.of(UNKNOWN));

    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory;
    private final LiteralInterpreter literalInterpreter;
    private final boolean isDescribe;

    // Cache from SQL type name to Type; every Type in the cache has a CAST defined from VARCHAR
    private final Cache<String, Type> varcharCastableTypeCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

    private final Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<FunctionCall>, Identifier> methodCallReceivers = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, List<Integer>> argumentBindings = new LinkedHashMap<>();
    private final Set<NodeRef<SubqueryExpression>> subqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Set<NodeRef<UniquePredicate>> uniquePredicates = new LinkedHashSet<>();
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

    private final List<OperandAndPredicate> subqueryInPredicates = new ArrayList<>();
    private final Map<NodeRef<Predicate>, PredicateCoercions> predicateCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
    private final Map<NodeRef<NullIfExpression>, Type> nullIfComparisonTypes = new LinkedHashMap<>();
    // The collation derivation of character string expressions whose derivation is not implicit from
    // their type: literals are coercible, COLLATE is explicit, and combinations can be either or none
    private final Map<NodeRef<Expression>, CollationShape> collationDerivations = new LinkedHashMap<>();
    // the shapes of the type variables of calls, from their arguments, until the result is typed
    private final Map<NodeRef<Expression>, Map<String, CollationShape>> argumentResultShapes = new LinkedHashMap<>();
    // the shapes of the values each lambda's parameters receive
    private final Map<NodeRef<LambdaExpression>, List<CollationShape>> lambdaArgumentShapes = new LinkedHashMap<>();
    private final Map<NodeRef<LambdaArgumentDeclaration>, CollationShape> lambdaParameterShapes = new LinkedHashMap<>();

    private static final Set<OperatorType> COMPARISON_OPERATORS = ImmutableSet.of(
            OperatorType.EQUAL,
            OperatorType.IDENTICAL,
            OperatorType.LESS_THAN,
            OperatorType.LESS_THAN_OR_EQUAL,
            OperatorType.COMPARISON_UNORDERED_LAST,
            OperatorType.COMPARISON_UNORDERED_FIRST,
            OperatorType.HASH_CODE,
            OperatorType.XX_HASH_64);

    // Built-in string functions declared over default-collation strings that search or compare
    // their arguments' contents, which would compare code points under any collation
    private static final Set<String> COLLATION_BLIND_COMPARISONS = ImmutableSet.of(
            "strpos",
            "starts_with",
            "ends_with",
            "replace",
            "split",
            "split_part",
            "split_to_map",
            "split_to_multimap",
            "translate",
            "levenshtein_distance",
            "hamming_distance",
            "regexp_like",
            "regexp_count",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_position",
            "regexp_replace",
            "regexp_split",
            "url_extract_parameter",
            "json_array_contains",
            "approx_set",
            "approx_most_frequent");
    private final List<OperandAndPredicate> quantifiedComparisons = new ArrayList<>();
    private final List<OperandAndPredicate> matchPredicates = new ArrayList<>();
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();
    private final Set<NodeRef<FunctionCall>> windowFunctions = new LinkedHashSet<>();
    private final Multimap<Analysis.TableAndBranch, String> tableColumnReferences = HashMultimap.create();

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
    private final Map<NodeRef<Expression>, Analysis.JsonSimplifiedAccessor> jsonSimplifiedAccessors = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedFunction> jsonInputFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, ResolvedFunction> jsonOutputFunctions = new LinkedHashMap<>();

    private final Session session;
    private final CharVarcharCoercion charVarcharCoercion;
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
            WarningCollector warningCollector)
    {
        this(plannerContext,
                accessControl,
                (_, correlationSupport) -> statementAnalyzerFactory.createStatementAnalyzer(
                        analysis,
                        session,
                        warningCollector,
                        correlationSupport),
                session,
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
        this.charVarcharCoercion = getCharVarcharCoercion(session);
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType, charVarcharCoercion);
        this.getPreanalyzedType = requireNonNull(getPreanalyzedType, "getPreanalyzedType is null");
        this.getResolvedWindow = requireNonNull(getResolvedWindow, "getResolvedWindow is null");
        this.functionResolver = plannerContext.getFunctionResolver(warningCollector);
    }

    public Map<NodeRef<Node>, ResolvedFunction> getResolvedFunctions()
    {
        return unmodifiableMap(resolvedFunctions);
    }

    public Map<NodeRef<FunctionCall>, Identifier> getMethodCallReceivers()
    {
        return unmodifiableMap(methodCallReceivers);
    }

    public Map<NodeRef<Expression>, List<Integer>> getArgumentBindings()
    {
        return unmodifiableMap(argumentBindings);
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

    /// The expressions with a string that has no collation because it combines different implicit
    /// collations. They cannot be compared, grouped, or ordered.
    public Set<NodeRef<Expression>> getUncollatedExpressions()
    {
        return collationDerivations.entrySet().stream()
                .filter(entry -> entry.getValue().firstNone().isPresent())
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());
    }

    public Map<NodeRef<NullIfExpression>, Type> getNullIfComparisonTypes()
    {
        return unmodifiableMap(nullIfComparisonTypes);
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

    public List<OperandAndPredicate> getSubqueryInPredicates()
    {
        return unmodifiableList(subqueryInPredicates);
    }

    public Map<NodeRef<Predicate>, PredicateCoercions> getPredicateCoercions()
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

    public boolean analyzeJsonWildcardTarget(Expression target, Scope scope, CorrelationSupport correlationSupport)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.analyzeJsonWildcardTarget(target, Context.notInLambda(scope, correlationSupport));
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
        List<Type> pathInvocationArgumentTypes = ImmutableList.of(JSON, plannerContext.getTypeManager().getType(new TypeDescriptor(SqlJsonPathType.NAME)), JSON_NO_PARAMETERS_ROW_TYPE);
        return visitor.analyzeJsonValueExpression(
                "JSON_TABLE",
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
        List<Type> pathInvocationArgumentTypes = ImmutableList.of(JSON, plannerContext.getTypeManager().getType(new TypeDescriptor(SqlJsonPathType.NAME)), JSON_NO_PARAMETERS_ROW_TYPE);
        return visitor.analyzeJsonQueryExpression(
                column,
                column.getWrapperBehavior(),
                column.getQuotesBehavior(),
                pathInvocationArgumentTypes,
                Optional.of(column.getType()),
                Optional.of(column.getFormat()),
                // JSON_TABLE columns always declare an explicit type, so the SR 1 implicit JSON return never applies
                false);
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

    public Set<NodeRef<UniquePredicate>> getUniquePredicates()
    {
        return unmodifiableSet(uniquePredicates);
    }

    public List<OperandAndPredicate> getQuantifiedComparisons()
    {
        return unmodifiableList(quantifiedComparisons);
    }

    public List<OperandAndPredicate> getMatchPredicates()
    {
        return ImmutableList.copyOf(matchPredicates);
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions()
    {
        return unmodifiableSet(windowFunctions);
    }

    public Multimap<Analysis.TableAndBranch, String> getTableColumnReferences()
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

    public Map<NodeRef<Expression>, Analysis.JsonSimplifiedAccessor> getJsonSimplifiedAccessors()
    {
        return jsonSimplifiedAccessors;
    }

    public Map<NodeRef<Expression>, ResolvedFunction> getJsonInputFunctions()
    {
        return jsonInputFunctions;
    }

    public Map<NodeRef<Node>, ResolvedFunction> getJsonOutputFunctions()
    {
        return jsonOutputFunctions;
    }

    private Set<Field> extractAccessibleFields(List<Field> fields)
    {
        ImmutableSet.Builder<Field> allowed = ImmutableSet.builder();

        ListMultimap<QualifiedObjectName, Field> fieldsByTable = ArrayListMultimap.create();
        for (Field field : fields) {
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                fieldsByTable.put(field.getOriginTable().get(), field);
            }
            else {
                // Fields with no origin table (aliases, derived columns) are always allowed
                allowed.add(field);
            }
        }

        for (Entry<QualifiedObjectName, Collection<Field>> entry : fieldsByTable.asMap().entrySet()) {
            QualifiedObjectName table = entry.getKey();
            Collection<Field> tableFields = entry.getValue();
            Set<String> accessibleColumns = accessControl.filterColumns(
                            session.toSecurityContext(),
                            table.catalogName(),
                            ImmutableMap.of(
                                    table.asSchemaTableName(), tableFields.stream()
                                            .map(field -> field.getOriginColumnName().orElseThrow())
                                            .collect(toImmutableSet())))
                    .getOrDefault(table.asSchemaTableName(), ImmutableSet.of());
            allowed.addAll(tableFields.stream()
                    .filter(field -> accessibleColumns.contains(field.getOriginColumnName().orElseThrow()))
                    .collect(toImmutableList()));
        }

        return allowed.build();
    }

    private class Visitor
    {
        private Type dispatch(Node root, Context context)
        {
            return switch (root) {
                case AllRows node -> visitExpression(node);
                case ArithmeticBinaryExpression node -> visitArithmeticBinary(node, context);
                case ArithmeticUnaryExpression node -> visitArithmeticUnary(node, context);
                case Array node -> visitArray(node, context);
                case ArrayWildcardSubscript node -> visitArrayWildcardSubscript(node, context);
                case AtLocal node -> visitAtLocal(node, context);
                case AtTimeZone node -> visitAtTimeZone(node, context);
                case BinaryLiteral node -> setExpressionType(node, VARBINARY);
                case BooleanLiteral node -> setExpressionType(node, BOOLEAN);
                case Cast node -> visitCast(node, context);
                case CollateExpression node -> visitCollateExpression(node, context);
                case CollationFor node -> visitCollationFor(node, context);
                case CoalesceExpression node -> visitCoalesceExpression(node, context);
                case CurrentCatalog node -> setExpressionType(node, VARCHAR);
                case CurrentDate node -> setExpressionType(node, DATE);
                case CurrentPath node -> setExpressionType(node, VARCHAR);
                case CurrentSchema node -> setExpressionType(node, VARCHAR);
                case CurrentTime node -> visitCurrentTime(node);
                case CurrentTimestamp node -> visitCurrentTimestamp(node);
                case CurrentUser node -> setExpressionType(node, VARCHAR);
                case DataType node -> visitExpression(node);
                case DecimalLiteral node -> visitDecimalLiteral(node);
                case DereferenceExpression node -> visitDereferenceExpression(node, context);
                case DoubleLiteral node -> setExpressionType(node, DOUBLE);
                case ExistsPredicate node -> visitExists(node, context);
                case Extract node -> visitExtract(node, context);
                case FieldReference node -> visitFieldReference(node, context);
                case Format node -> visitFormat(node, context);
                case FunctionCall node -> visitFunctionCall(node, context);
                case GenericLiteral node -> visitGenericLiteral(node);
                case GroupingOperation node -> visitGroupingOperation(node, context);
                case Identifier node -> visitIdentifier(node, context);
                case IfExpression node -> visitIfExpression(node, context);
                case InListExpression node -> visitExpression(node);
                case IntervalLiteral node -> visitIntervalLiteral(node);
                case IntervalValueExpression node -> visitIntervalValueExpression(node, context);
                case JsonArray node -> visitJsonArray(node, context);
                case JsonConstructor node -> visitJsonConstructor(node, context);
                case JsonExists node -> visitJsonExists(node, context);
                case JsonObject node -> visitJsonObject(node, context);
                case JsonQuery node -> visitJsonQuery(node, context);
                case JsonSerialize node -> visitJsonSerialize(node, context);
                case JsonValue node -> visitJsonValue(node, context);
                case LambdaArgumentDeclaration node -> visitExpression(node);
                case LambdaExpression node -> visitLambdaExpression(node, context);
                case LocalTime node -> visitLocalTime(node);
                case LocalTimestamp node -> visitLocalTimestamp(node);
                case LogicalExpression node -> visitLogicalExpression(node, context);
                case LongLiteral node -> visitLongLiteral(node);
                case MethodCall node -> visitMethodCall(node, context);
                case NotExpression node -> visitNotExpression(node, context);
                case NullIfExpression node -> visitNullIfExpression(node, context);
                case NullLiteral node -> setExpressionType(node, UNKNOWN);
                case Overlay node -> visitOverlay(node, context);
                case Parameter node -> visitParameter(node, context);
                case Predicated node -> visitPredicated(node, context);
                case Row node -> visitRow(node, context);
                case SearchedCaseExpression node -> visitSearchedCaseExpression(node, context);
                case SimpleCaseExpression node -> visitSimpleCaseExpression(node, context);
                case StaticMethodCall node -> visitStaticMethodCall(node, context);
                case StringLiteral node -> visitStringLiteral(node);
                case SubqueryExpression node -> visitSubqueryExpression(node, context);
                case SubscriptExpression node -> visitSubscriptExpression(node, context);
                case Trim node -> visitTrim(node, context);
                case TryExpression node -> visitTryExpression(node, context);
                case UniquePredicate node -> visitUniquePredicate(node, context);
                case WhenClause node -> visitExpression(node);
                case WindowOperation node -> visitWindowOperation(node, context);
                default -> throw semanticException(NOT_SUPPORTED, root, "not yet implemented: %s", root.getClass().getName());
            };
        }

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

        public Type process(Node node, @Nullable Context context)
        {
            if (node instanceof Expression expression) {
                // don't double process a node
                Type type = expressionTypes.get(NodeRef.of(expression));
                if (type != null) {
                    return type;
                }
            }
            return dispatch(node, context);
        }

        private Type visitRow(Row node, Context context)
        {
            List<RowType.Field> fields = node.getFields().stream()
                    .map(field -> new RowType.Field(
                            field.getName().map(Identifier::getCanonicalValue),
                            process(field.getExpression(), context)))
                    .collect(toImmutableList());

            Type type = RowType.from(fields);
            recordShape(node, new RowShape(node.getFields().stream()
                    .map(field -> shape(field.getExpression()))
                    .collect(toImmutableList())), type);
            return setExpressionType(node, type);
        }

        private Type visitCurrentTime(CurrentTime node)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimeWithTimeZoneType::createTimeWithTimeZoneType)
                            .orElse(TIME_TZ_MILLIS));
        }

        private Type visitCurrentTimestamp(CurrentTimestamp node)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimestampWithTimeZoneType::createTimestampWithTimeZoneType)
                            .orElse(TIMESTAMP_TZ_MILLIS));
        }

        private Type visitLocalTime(LocalTime node)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimeType::createTimeType)
                            .orElse(TIME_MILLIS));
        }

        private Type visitLocalTimestamp(LocalTimestamp node)
        {
            return setExpressionType(
                    node,
                    node.getPrecision()
                            .map(TimestampType::createTimestampType)
                            .orElse(TIMESTAMP_MILLIS));
        }

        private Type visitIdentifier(Identifier node, Context context)
        {
            Scope scope = context.getScope();

            // Lazy extraction to avoid unnecessary work
            Supplier<Set<Field>> accessibleFields = () -> extractAccessibleFields(scope.collectSuggestionFields());
            ResolvedField resolvedField = scope.resolveField(node, QualifiedName.of(node.getValue()), accessibleFields);

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
                    CollationShape parameterShape = lambdaParameterShapes.get(NodeRef.of(lambdaArgumentDeclaration));
                    if (parameterShape != null) {
                        collationDerivations.put(NodeRef.of(node), parameterShape);
                    }
                    return setExpressionType(node, field.getType());
                }
            }

            recordColumnUsage(resolvedField);

            ResolvedField previous = columnReferences.put(NodeRef.of(node), resolvedField);
            checkState(previous == null, "%s already known to refer to %s", node, previous);

            return setExpressionType(node, field.getType());
        }

        private Type visitDereferenceExpression(DereferenceExpression node, Context context)
        {
            boolean hasWildcard = isQualifiedAllFieldsReference(node);
            QualifiedName qualifiedName = hasWildcard ? null : DereferenceExpression.getQualifiedName(node);

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
            }

            if (!context.isPatternRecognition()) {
                Optional<Type> type = tryAnalyzeJsonSimplifiedAccessor(node, context);
                if (type.isPresent()) {
                    return setExpressionType(node, type.get());
                }
            }

            if (hasWildcard) {
                throw semanticException(NOT_SUPPORTED, node, "<identifier>.* not allowed in this context");
            }

            if (qualifiedName != null && !context.getScope().isColumnReference(qualifiedName)) {
                throw missingAttributeException(node, qualifiedName);
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType rowType)) {
                throw semanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            Identifier field = node.getField().orElseThrow();
            String fieldName = field.getValue();

            boolean foundFieldName = false;
            Type rowFieldType = null;
            int fieldIndex = -1;
            for (int i = 0; i < rowType.getFields().size(); i++) {
                RowType.Field rowField = rowType.getFields().get(i);
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    if (foundFieldName) {
                        throw semanticException(AMBIGUOUS_NAME, field, "Ambiguous row field reference: %s", fieldName);
                    }
                    foundFieldName = true;
                    rowFieldType = rowField.getType();
                    fieldIndex = i;
                }
            }

            if (rowFieldType == null) {
                throw invalidReferenceException(node, Optional.ofNullable(qualifiedName)
                        .orElseGet(() -> QualifiedName.of(fieldName)));
            }

            recordFieldShape(node, node.getBase(), fieldIndex, rowFieldType);
            return setExpressionType(node, rowFieldType);
        }

        private Optional<Type> tryAnalyzeJsonSimplifiedAccessor(Expression expression, Context context)
        {
            return JsonAccessorChain.walk(expression)
                    .flatMap(chain -> tryRegisterJsonQueryAccessor(expression, chain, Optional.empty(), context));
        }

        boolean analyzeJsonWildcardTarget(Expression target, Context context)
        {
            return JsonAccessorChain.walkForWildcard(target)
                    .flatMap(chain -> tryRegisterJsonQueryAccessor(target, chain, Optional.of(AccessorStep.WildcardMember.INSTANCE), context))
                    .isPresent();
        }

        private Optional<Type> tryRegisterJsonQueryAccessor(Expression expression, JsonAccessorChain chain, Optional<AccessorStep> trailingStep, Context context)
        {
            boolean registered = tryRegisterJsonAccessor(expression, chain, trailingStep, context, (column, pathAnalysis) -> new Analysis.JsonSimplifiedAccessor.Query(
                    column,
                    pathAnalysis,
                    getInputFunction(VARCHAR, JsonFormat.JSON, expression),
                    plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_QUERY_FUNCTION_NAME, ImmutableList.of(
                            JSON,
                            plannerContext.getTypeManager().getType(new TypeDescriptor(SqlJsonPathType.NAME)),
                            JSON_NO_PARAMETERS_ROW_TYPE,
                            TINYINT,
                            TINYINT,
                            TINYINT)),
                    getOutputFunction(VARCHAR, JsonFormat.JSON, expression)));
            return registered ? Optional.of(setExpressionType(expression, VARCHAR)) : Optional.empty();
        }

        private Optional<Type> tryRegisterJsonValueAccessor(Expression expression, JsonAccessorChain chain, Type returnedType, Context context)
        {
            boolean registered = tryRegisterJsonAccessor(expression, chain, Optional.empty(), context, (column, pathAnalysis) -> new Analysis.JsonSimplifiedAccessor.Value(
                    column,
                    pathAnalysis,
                    getInputFunction(VARCHAR, JsonFormat.JSON, expression),
                    plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_VALUE_FUNCTION_NAME, ImmutableList.of(
                            JSON,
                            plannerContext.getTypeManager().getType(new TypeDescriptor(SqlJsonPathType.NAME)),
                            JSON_NO_PARAMETERS_ROW_TYPE,
                            returnedType,
                            TINYINT,
                            new FunctionType(ImmutableList.of(), returnedType),
                            TINYINT,
                            new FunctionType(ImmutableList.of(), returnedType))),
                    returnedType));
            return registered ? Optional.of(returnedType) : Optional.empty();
        }

        @FunctionalInterface
        private interface JsonAccessorRecipeBuilder
        {
            Analysis.JsonSimplifiedAccessor build(ResolvedField column, JsonPathAnalysis pathAnalysis);
        }

        /// @return `true` when a simplified-accessor recipe was registered
        ///         for `expression`; `false` when no prefix of the chain
        ///         resolved to a JSON column.
        private boolean tryRegisterJsonAccessor(
                Expression expression,
                JsonAccessorChain chain,
                Optional<AccessorStep> trailingStep,
                Context context,
                JsonAccessorRecipeBuilder recipe)
        {
            Scope scope = context.getScope();
            List<Identifier> prefix = chain.prefix();
            for (int prefixLength = prefix.size(); prefixLength >= 1; prefixLength--) {
                Optional<ResolvedField> resolved = scope.tryResolveField(expression, QualifiedName.of(prefix.subList(0, prefixLength)));
                if (resolved.isEmpty()) {
                    continue;
                }
                if (!resolved.get().getType().equals(JSON)) {
                    return false;
                }
                ResolvedField column = resolved.get();
                registerSimplifiedAccessorColumn(column, chain, prefixLength, expression, context);
                JsonPathAnalysis pathAnalysis = analyzeSimplifiedAccessorPath(expression, chain, prefixLength, trailingStep);
                jsonSimplifiedAccessors.put(NodeRef.of(expression), recipe.build(column, pathAnalysis));
                return true;
            }
            return false;
        }

        private JsonPathAnalysis analyzeSimplifiedAccessorPath(Expression expression, JsonAccessorChain chain, int prefixLength, Optional<AccessorStep> trailingStep)
        {
            return new JsonPathAnalyzer(
                    plannerContext.getMetadata(),
                    session,
                    createConstantAnalyzer(plannerContext, accessControl, session, ExpressionAnalyzer.this.parameters, WarningCollector.NOOP))
                    .analyzeJsonPath(
                            JsonAccessorChain.buildPath(ImmutableList.<AccessorStep>builder()
                                    .addAll(chain.prefix().subList(prefixLength, chain.prefix().size()).stream()
                                            .<AccessorStep>map(AccessorStep.Member::new)
                                            .iterator())
                                    .addAll(chain.outerSteps())
                                    .addAll(trailingStep.stream().iterator())
                                    .build()),
                            expression.getLocation().orElseThrow(() -> new IllegalStateException("missing NodeLocation in accessor")));
        }

        private void registerSimplifiedAccessorColumn(ResolvedField column, JsonAccessorChain chain, int prefixLength, Expression expression, Context context)
        {
            Optional<Expression> matched = chain.matchedPrefixExpression(prefixLength);
            if (matched.isEmpty()) {
                if (!column.isLocal()) {
                    throw semanticException(NOT_SUPPORTED, expression, "JSON simplified accessor over a column from an outer scope is not supported");
                }
                recordColumnUsage(column);
                return;
            }
            // Route the matched sub-expression through the regular visitor so
            // it lands in `analysis.columnReferences`, where the outer-scope
            // correlation machinery picks it up.
            process(matched.get(), context);
        }

        private void recordColumnUsage(ResolvedField resolvedField)
        {
            Field field = resolvedField.getField();
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(new Analysis.TableAndBranch(field.getOriginTable().get(), field.getOriginBranch()), field.getOriginColumnName().get());
            }
            sourceFields.add(field);
            FieldId.from(resolvedField).getRelationId()
                    .getSourceNode()
                    .ifPresent(source -> referencedFields.put(NodeRef.of(source), field));
        }

        private Optional<Type> tryAnalyzeJsonItemMethodAccessor(FunctionCall node, Context context)
        {
            if (context.isPatternRecognition() || context.isInWindow()) {
                return Optional.empty();
            }
            if (node.isDistinct()
                    || node.getFilter().isPresent()
                    || node.getOrderBy().isPresent()
                    || node.getWindow().isPresent()
                    || node.getProcessingMode().isPresent()
                    || node.getNullTreatment().isPresent()) {
                return Optional.empty();
            }
            if (node.hasNamedArguments()) {
                // item methods define only positional precision literals; a named argument
                // means this is an ordinary function call
                return Optional.empty();
            }
            List<Identifier> parts = node.getName().getOriginalParts();
            if (parts.size() < 2 || parts.getLast().isDelimited()) {
                // item-method names are regular identifiers (case-insensitive); a delimited
                // identifier denotes an ordinary function or member name, never an item method
                return Optional.empty();
            }
            Optional<Type> returnedType = resolveJsonItemMethodReturnType(parts.getLast().getValue(), node.argumentValues());
            if (returnedType.isEmpty()) {
                return Optional.empty();
            }
            // FunctionCall shape has no Expression sub-node for the receiver
            // chain (the receiver is the qualified name's prefix). An empty
            // cursor list keeps `matchedPrefixExpression` empty so outer-scope
            // resolution falls through to the local-only path.
            List<Identifier> receiverParts = parts.subList(0, parts.size() - 1);
            JsonAccessorChain chain = new JsonAccessorChain(receiverParts, ImmutableList.of(), ImmutableList.of());
            return tryRegisterJsonValueAccessor(node, chain, returnedType.get(), context);
        }

        private Optional<Type> tryAnalyzeJsonItemMethodAccessor(MethodCall node, Context context)
        {
            if (context.isPatternRecognition() || context.isInWindow()) {
                return Optional.empty();
            }
            if (node.getMethod().isDelimited()) {
                // item-method names are regular identifiers (case-insensitive); a delimited
                // identifier denotes an ordinary method name, never an item method
                return Optional.empty();
            }
            if (node.getArguments().stream().anyMatch(argument -> argument.getName().isPresent())) {
                // named arguments denote an ordinary method call, never an item method
                return Optional.empty();
            }
            Optional<Type> returnedType = resolveJsonItemMethodReturnType(
                    node.getMethod().getValue(),
                    node.getArguments().stream().map(CallArgument::getValue).collect(toImmutableList()));
            if (returnedType.isEmpty()) {
                return Optional.empty();
            }
            Optional<JsonAccessorChain> chain = JsonAccessorChain.walk(node.getReceiver());
            if (chain.isEmpty()) {
                return Optional.empty();
            }
            return tryRegisterJsonValueAccessor(node, chain.get(), returnedType.get(), context);
        }

        private static Optional<Type> resolveJsonItemMethodReturnType(String methodName, List<Expression> arguments)
        {
            List<Integer> integerArguments = new ArrayList<>();
            for (Expression argument : arguments) {
                // precision and scale arguments fit in an int; a literal outside that range is not
                // one of the item-method forms, so the call falls through to method resolution
                // instead of silently wrapping into a different in-range value
                if (!(argument instanceof LongLiteral literal) || (int) literal.getParsedValue() != literal.getParsedValue()) {
                    return Optional.empty();
                }
                integerArguments.add((int) literal.getParsedValue());
            }
            return switch (methodName.toLowerCase(ENGLISH)) {
                case "bigint" -> integerArguments.isEmpty() ? Optional.of(BIGINT) : Optional.empty();
                case "boolean" -> integerArguments.isEmpty() ? Optional.of(BOOLEAN) : Optional.empty();
                case "date" -> integerArguments.isEmpty() ? Optional.of(DATE) : Optional.empty();
                case "integer" -> integerArguments.isEmpty() ? Optional.of(INTEGER) : Optional.empty();
                case "number" -> integerArguments.isEmpty() ? Optional.of(DOUBLE) : Optional.empty();
                case "string" -> integerArguments.isEmpty() ? Optional.of(VARCHAR) : Optional.empty();
                case "decimal" -> switch (integerArguments.size()) {
                    case 0 -> Optional.of(DecimalType.createDecimalType());
                    case 1 -> Optional.of(DecimalType.createDecimalType(integerArguments.get(0)));
                    case 2 -> Optional.of(DecimalType.createDecimalType(integerArguments.get(0), integerArguments.get(1)));
                    default -> Optional.empty();
                };
                case "time" -> switch (integerArguments.size()) {
                    case 0 -> Optional.of(createTimeType(3));
                    case 1 -> Optional.of(createTimeType(integerArguments.get(0)));
                    default -> Optional.empty();
                };
                case "time_tz" -> switch (integerArguments.size()) {
                    case 0 -> Optional.of(createTimeWithTimeZoneType(3));
                    case 1 -> Optional.of(createTimeWithTimeZoneType(integerArguments.get(0)));
                    default -> Optional.empty();
                };
                case "timestamp" -> switch (integerArguments.size()) {
                    case 0 -> Optional.of(createTimestampType(3));
                    case 1 -> Optional.of(createTimestampType(integerArguments.get(0)));
                    default -> Optional.empty();
                };
                case "timestamp_tz" -> switch (integerArguments.size()) {
                    case 0 -> Optional.of(createTimestampWithTimeZoneType(3));
                    case 1 -> Optional.of(createTimestampWithTimeZoneType(integerArguments.get(0)));
                    default -> Optional.empty();
                };
                default -> Optional.empty();
            };
        }

        private Type visitNotExpression(NotExpression node, Context context)
        {
            coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            return setExpressionType(node, BOOLEAN);
        }

        private Type visitLogicalExpression(LogicalExpression node, Context context)
        {
            for (Expression term : node.getTerms()) {
                coerceType(context, term, BOOLEAN, "Logical expression term");
            }

            return setExpressionType(node, BOOLEAN);
        }

        private Type visitPredicated(Predicated node, Context context)
        {
            return switch (node.getPredicate()) {
                case BetweenPredicate predicate -> analyzeBetween(node.getValue(), predicate, node, context);
                case BooleanTestPredicate _ -> analyzeBooleanTest(node.getValue(), node, context);
                case ComparisonPredicate predicate -> analyzeComparison(node.getValue(), predicate, node, context);
                case DistinctFromPredicate predicate -> analyzeDistinctFrom(node.getValue(), predicate, node, context);
                case InPredicate predicate -> analyzeIn(node.getValue(), predicate, node, context);
                case IsNullPredicate _ -> analyzeIsNull(node.getValue(), node, context);
                case LikePredicate predicate -> analyzeLike(node.getValue(), predicate, node, context);
                case MatchPredicate predicate -> analyzeMatchPredicate(node.getValue(), predicate, node, context);
                case OverlapsPredicate predicate -> analyzeOverlaps(node.getValue(), predicate, node, context);
                case QuantifiedComparisonPredicate predicate -> analyzeQuantifiedComparison(node.getValue(), predicate, node, context);
            };
        }

        private Type analyzeBetween(Expression value, BetweenPredicate predicate, Expression anchor, Context context)
        {
            process(value, context);
            process(predicate.getMin(), context);
            process(predicate.getMax(), context);
            List<Type> aligned = determineCollation(anchor, ImmutableList.of(value, predicate.getMin(), predicate.getMax()));
            Type valueType = aligned.get(0);
            Type minType = aligned.get(1);
            Type maxType = aligned.get(2);

            Optional<Type> commonType = typeCoercion.getCommonSuperType(valueType, minType)
                    .flatMap(type -> typeCoercion.getCommonSuperType(type, maxType));

            if (commonType.isEmpty()) {
                throw semanticException(TYPE_MISMATCH, anchor, "Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType);
            }

            if (!commonType.get().isOrderable()) {
                throw semanticException(TYPE_MISMATCH, anchor, "Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType);
            }

            if (!valueType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(value, commonType.get());
            }
            if (!minType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(predicate.getMin(), commonType.get());
            }
            if (!maxType.equals(commonType.get())) {
                addOrReplaceExpressionCoercion(predicate.getMax(), commonType.get());
            }
            return setExpressionType(anchor, BOOLEAN);
        }

        private Type analyzeComparison(Expression value, ComparisonPredicate predicate, Expression anchor, Context context)
        {
            OperatorType operatorType = switch (predicate.getOperator()) {
                case EQUAL, NOT_EQUAL -> OperatorType.EQUAL;
                case LESS_THAN, GREATER_THAN -> OperatorType.LESS_THAN;
                case LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL -> OperatorType.LESS_THAN_OR_EQUAL;
            };
            return getOperator(context, anchor, operatorType, value, predicate.getRight());
        }

        private Type analyzeDistinctFrom(Expression value, DistinctFromPredicate predicate, Expression anchor, Context context)
        {
            return getOperator(context, anchor, OperatorType.IDENTICAL, value, predicate.getRight());
        }

        private Type analyzeIsNull(Expression value, Expression anchor, Context context)
        {
            process(value, context);
            return setExpressionType(anchor, BOOLEAN);
        }

        private Type analyzeBooleanTest(Expression value, Expression anchor, Context context)
        {
            coerceType(context, value, BOOLEAN, "Boolean test value");
            return setExpressionType(anchor, BOOLEAN);
        }

        private Type visitNullIfExpression(NullIfExpression node, Context context)
        {
            Type resultType = process(node.getFirst(), context);
            Type secondOperandType = process(node.getSecond(), context);
            // The comparison uses the collations determined at each string position, while the result
            // keeps the first operand's value and type, so the operands are compared without relabeling them
            Optional<CollationShape> determined = determinedShape(node, ImmutableList.of(node.getFirst(), node.getSecond()));
            Type firstType = determined.map(shape -> CollationShapes.applyTo(resultType, shape, plannerContext.getTypeManager())).orElse(resultType);
            Type secondType = determined.map(shape -> CollationShapes.applyTo(secondOperandType, shape, plannerContext.getTypeManager())).orElse(secondOperandType);
            inheritCollation(node, node.getFirst());

            Type comparisonType = typeCoercion.getCommonSuperType(firstType, secondType)
                    .orElseThrow(() -> semanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType));
            nullIfComparisonTypes.put(NodeRef.of(node), comparisonType);

            return setExpressionType(node, resultType);
        }

        private Type visitIfExpression(IfExpression node, Context context)
        {
            coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
                inheritCollation(node, node.getTrueValue());
            }

            return setExpressionType(node, type);
        }

        private Type visitSearchedCaseExpression(SearchedCaseExpression node, Context context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                switch (whenClause.getMatch()) {
                    case WhenClause.Operand operand -> coerceType(context, operand.expression(), BOOLEAN, "CASE WHEN clause");
                    case WhenClause.Partial _ -> throw new IllegalStateException("WHEN with a predicate fragment is not allowed in searched CASE; should have been rejected by the parser");
                }
            }

            Type type = coerceToSingleType(
                    context,
                    node,
                    "All CASE results",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context)
        {
            coerceCaseOperandToSingleType(node, context);

            Type type = coerceToSingleType(
                    context,
                    node,
                    "All CASE results",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private void coerceCaseOperandToSingleType(SimpleCaseExpression node, Context context)
        {
            Expression operand = node.getOperand();
            Type operandType = process(operand, context);

            List<WhenClause> whenClauses = node.getWhenClauses();

            // Type-check each F262 predicate-fragment WHEN through the shared predicate analysis
            // methods, passing the case operand as the implicit LHS and the WHEN clause as the
            // node anchoring types, errors, and subquery registration. This validates operator
            // resolution, coerces LIKE patterns, and registers subqueries.
            for (WhenClause whenClause : whenClauses) {
                if (whenClause.getMatch() instanceof WhenClause.Partial(Predicate predicate)) {
                    switch (predicate) {
                        case BetweenPredicate fragment -> analyzeBetween(operand, fragment, whenClause, context);
                        case BooleanTestPredicate _ -> analyzeBooleanTest(operand, whenClause, context);
                        case ComparisonPredicate fragment -> analyzeComparison(operand, fragment, whenClause, context);
                        case DistinctFromPredicate fragment -> analyzeDistinctFrom(operand, fragment, whenClause, context);
                        case InPredicate fragment -> analyzeIn(operand, fragment, whenClause, context);
                        case IsNullPredicate _ -> analyzeIsNull(operand, whenClause, context);
                        case LikePredicate fragment -> analyzeLike(operand, fragment, whenClause, context);
                        case MatchPredicate fragment -> analyzeMatchPredicate(operand, fragment, whenClause, context);
                        case OverlapsPredicate fragment -> analyzeOverlaps(operand, fragment, whenClause, context);
                        case QuantifiedComparisonPredicate fragment -> analyzeQuantifiedComparison(operand, fragment, whenClause, context);
                    }
                }
            }

            // The case operand is evaluated once and shared by every clause, so all clauses must
            // agree on a single operand type. Reconcile the operand with every value it is
            // directly compared against — equality WHEN values and the value parts of
            // comparison / BETWEEN / IN-list / IS DISTINCT FROM fragments — into one common
            // supertype, then coerce the operand and those values to it. This overrides the
            // narrower per-clause coercions applied above, which each saw only their own clause.
            List<Expression> comparedValues = new ArrayList<>();
            for (WhenClause whenClause : whenClauses) {
                switch (whenClause.getMatch()) {
                    case WhenClause.Operand match -> comparedValues.add(match.expression());
                    case WhenClause.Partial(Predicate predicate) -> comparedValues.addAll(operandComparedValues(predicate));
                }
            }
            if (comparedValues.isEmpty()) {
                return;
            }

            for (Expression value : comparedValues) {
                process(value, context);
            }
            // the operand is compared with every value, so they are compared under one collation
            List<Type> aligned = determineCollation(node, ImmutableList.<Expression>builder()
                    .add(operand)
                    .addAll(comparedValues)
                    .build());
            Type alignedOperandType = aligned.getFirst();
            List<Type> valueTypes = new ArrayList<>(aligned.subList(1, aligned.size()));
            Type commonType = alignedOperandType;
            for (int i = 0; i < comparedValues.size(); i++) {
                Expression value = comparedValues.get(i);
                Type valueType = valueTypes.get(i);
                commonType = typeCoercion.getCommonSuperType(commonType, valueType)
                        .orElseThrow(() -> semanticException(TYPE_MISMATCH, value, "CASE operand type does not match WHEN clause operand type: %s vs %s", alignedOperandType, valueType));
            }

            if (!commonType.equals(operandType)) {
                addOrReplaceExpressionCoercion(operand, commonType);
            }
            for (int i = 0; i < valueTypes.size(); i++) {
                if (!valueTypes.get(i).equals(commonType)) {
                    addOrReplaceExpressionCoercion(comparedValues.get(i), commonType);
                }
            }
        }

        /// The values inside a F262 predicate fragment that are directly compared against the case
        /// operand and must therefore reconcile to a common type with it. LIKE and IS NULL impose
        /// no such value; subquery-bearing IN and quantified-comparison fragments reconcile through
        /// the regular predicate analysis path instead.
        private static List<Expression> operandComparedValues(Predicate predicate)
        {
            return switch (predicate) {
                case ComparisonPredicate comparison -> ImmutableList.of(comparison.getRight());
                case BetweenPredicate between -> ImmutableList.of(between.getMin(), between.getMax());
                case DistinctFromPredicate distinct -> ImmutableList.of(distinct.getRight());
                case InPredicate in when in.getValueList() instanceof InListExpression list -> list.getValues();
                default -> ImmutableList.of();
            };
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

        private Type visitCoalesceExpression(CoalesceExpression node, Context context)
        {
            Type type = coerceToSingleType(context, node, "All COALESCE operands", node.getOperands());

            return setExpressionType(node, type);
        }

        private Type visitArithmeticUnary(ArithmeticUnaryExpression node, Context context)
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

        private Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        private Type analyzeLike(Expression value, LikePredicate predicate, Expression anchor, Context context)
        {
            Type valueType = process(value, context);
            if (!(valueType instanceof CharType) && !(valueType instanceof VarcharType)) {
                coerceType(context, value, VARCHAR, "Left side of LIKE expression");
            }

            Type patternType = process(predicate.getPattern(), context);
            if (!(patternType instanceof VarcharType)) {
                // TODO can pattern be of char type?
                coerceType(context, predicate.getPattern(), VARCHAR, "Pattern for LIKE expression");
            }
            if (predicate.getEscape().isPresent()) {
                Expression escape = predicate.getEscape().get();
                Type escapeType = process(escape, context);
                if (!(escapeType instanceof VarcharType)) {
                    // TODO can escape be of char type?
                    coerceType(context, escape, VARCHAR, "Escape for LIKE expression");
                }
            }

            // Pattern matching compares code points; which collations LIKE supports is
            // implementation-defined (ISO/IEC 9075-2 8.5), and only the default collation is
            Optional<Collation> collation = determinedCollation(anchor, ImmutableList.of(value, predicate.getPattern()));
            if (collation.isPresent() && !collation.get().isDefault()) {
                throw semanticException(NOT_SUPPORTED, anchor, "LIKE does not support collation %s", collation.get().name());
            }

            return setExpressionType(anchor, BOOLEAN);
        }

        private Type visitSubscriptExpression(SubscriptExpression node, Context context)
        {
            if (!context.isPatternRecognition()) {
                Optional<Type> type = tryAnalyzeJsonSimplifiedAccessor(node, context);
                if (type.isPresent()) {
                    return setExpressionType(node, type.get());
                }
            }

            Type baseType = process(node.getBase(), context);
            // Subscript on Row hasn't got a dedicated operator. Its Type is resolved by hand.
            if (baseType instanceof RowType rowType) {
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
                List<Type> rowTypes = rowType.getFieldTypes();
                if (indexValue > rowTypes.size()) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, node.getIndex(), "Subscript index out of bounds: %s, max value is %s", indexValue, rowTypes.size());
                }
                recordFieldShape(node, node.getBase(), indexValue - 1, rowTypes.get(indexValue - 1));
                return setExpressionType(node, rowTypes.get(indexValue - 1));
            }

            // Subscript on Array or Map uses an operator to resolve Type.
            if (baseType instanceof MapType) {
                // a map subscript compares the index with the keys
                Type indexType = process(node.getIndex(), context);
                if (shape(node.getBase()) instanceof MapShape(CollationShape key, CollationShape _) && indexType instanceof VarcharType) {
                    determineKeyCollation(node, key, node.getIndex());
                }
            }
            Type type = getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
            switch (shape(node.getBase())) {
                case ArrayShape(CollationShape element) -> recordShape(node, element, type);
                case MapShape(CollationShape _, CollationShape value) -> recordShape(node, value, type);
                default -> {}
            }
            return type;
        }

        private Type visitArrayWildcardSubscript(ArrayWildcardSubscript node, Context context)
        {
            if (!context.isPatternRecognition()) {
                Optional<Type> type = tryAnalyzeJsonSimplifiedAccessor(node, context);
                if (type.isPresent()) {
                    return setExpressionType(node, type.get());
                }
            }
            // Surface any column-not-found or type errors on the base before
            // reporting the catch-all "[*] not allowed" message — otherwise a
            // mistyped column reads as a syntax complaint.
            process(node.getBase(), context);
            throw semanticException(NOT_SUPPORTED, node, "[*] array wildcard accessor is only allowed over a JSON simplified accessor chain");
        }

        private Type visitArray(Array node, Context context)
        {
            Type type = coerceToSingleType(context, node, "All ARRAY elements", node.getValues());
            Type arrayType = plannerContext.getTypeManager().getParameterizedType(ARRAY.getName(), ImmutableList.of(TypeParameter.typeParameter(type.getTypeDescriptor())));
            // coerceToSingleType records the elements' combined shape on the array node
            CollationShape element = Optional.ofNullable(collationDerivations.remove(NodeRef.of(node))).orElseGet(() -> CollationShapes.implicit(type));
            recordShape(node, new ArrayShape(element), arrayType);
            return setExpressionType(node, arrayType);
        }

        private Type visitStringLiteral(StringLiteral node)
        {
            VarcharType type = VarcharType.createVarcharType(node.length());
            collationDerivations.put(NodeRef.of(node), new Leaf(Collated.coercible()));
            return setExpressionType(node, type);
        }

        private Type visitLongLiteral(LongLiteral node)
        {
            if (node.getParsedValue() >= Integer.MIN_VALUE && node.getParsedValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }

        private Type visitDecimalLiteral(DecimalLiteral node)
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

        private Type visitGenericLiteral(GenericLiteral node)
        {
            return setExpressionType(
                    node,
                    switch (node.getType()) {
                        case String value when value.equalsIgnoreCase("CHAR") -> CharType.createCharType(node.getValue().codePointCount(0, node.getValue().length()));
                        case String value when value.equalsIgnoreCase("TIME") -> processTimeLiteral(node);
                        case String value when value.equalsIgnoreCase("TIMESTAMP") -> processTimestampLiteral(node);
                        default -> {
                            Type type;
                            try {
                                type = uncheckedCacheGet(varcharCastableTypeCache, node.getType(), () -> {
                                    Type resolvedType;
                                    try {
                                        resolvedType = plannerContext.getTypeManager().fromSqlType(node.getType());
                                    }
                                    catch (TypeNotFoundException e) {
                                        throw semanticException(TYPE_NOT_FOUND, node, "Unknown type: %s", node.getType());
                                    }

                                    if (!JSON.equals(resolvedType)) {
                                        try {
                                            plannerContext.getMetadata().getCoercion(charVarcharCoercion, VARCHAR, resolvedType);
                                        }
                                        catch (IllegalArgumentException e) {
                                            throw semanticException(INVALID_LITERAL, node, "No literal form for type %s", resolvedType);
                                        }
                                    }
                                    return resolvedType;
                                });
                            }
                            catch (UncheckedExecutionException e) {
                                throwIfUnchecked(e.getCause());
                                throw new RuntimeException(e.getCause());
                            }
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

        private Type visitIntervalLiteral(IntervalLiteral node)
        {
            validateIntervalQualifier(node.qualifier());

            IntervalDataType dataType = new IntervalDataType(node.getLocation(), node.qualifier());
            Type type;
            try {
                TypeDescriptor descriptor = toTypeDescriptor(dataType);
                if (intervalQualifierHasInferableFractionalPrecision(dataType)) {
                    descriptor = inferIntervalFractionalPrecision(descriptor, node.getValue());
                }
                // Infer the leading width from the value after rounding to the fractional precision.
                if (!intervalQualifierHasPrecision(dataType)) {
                    descriptor = inferIntervalLeadingPrecision(descriptor, node.getValue(), node.getSign().multiplier());
                }
                type = plannerContext.getTypeManager().getType(descriptor);
                literalInterpreter.evaluate(node, type);
            }
            catch (TrinoException e) {
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_LITERAL, node, e, "'%s' is not a valid INTERVAL literal", node.getValue());
            }
            return setExpressionType(node, type);
        }

        private Type visitFunctionCall(FunctionCall node, Context context)
        {
            // A JSON item method on a JSON-typed receiver resolves as the item method
            // (SQL:2023 §6.36 case 3.a), shadowing any real method of the same name —
            // the same precedence as visitMethodCall
            Optional<Type> asJsonItemMethod = tryAnalyzeJsonItemMethodAccessor(node, context);
            if (asJsonItemMethod.isPresent()) {
                return setExpressionType(node, asJsonItemMethod.get());
            }

            // SQL:2023 6.3 Syntax Rule 2: a non-parenthesized value expression primary
            // of the form A.B(args) is treated as a method invocation if it satisfies
            // the rules for one; otherwise it is a routine invocation.
            Optional<Type> asMethod = tryResolveAsInstanceMethod(node, context);
            if (asMethod.isPresent()) {
                return asMethod.get();
            }

            boolean isAggregation = functionResolver.isAggregationFunction(session, node.getName(), accessControl);
            boolean isRowPatternCount = context.isPatternRecognition() &&
                    isAggregation &&
                    node.getName().getSuffix().equalsIgnoreCase("count");
            // argument of the form `label.*` is only allowed for row pattern count function
            node.argumentValues().stream()
                    .filter(DereferenceExpression::isQualifiedAllFieldsReference)
                    .findAny()
                    .ifPresent(allRowsReference -> {
                        if (!isRowPatternCount || node.getArguments().size() > 1) {
                            throw semanticException(INVALID_FUNCTION_ARGUMENT, allRowsReference, "label.* syntax is only supported as the only argument of row pattern count function");
                        }
                    });

            if (node.hasNamedArguments() && isPatternRecognitionFunction(node)) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "Named arguments are not supported for pattern recognition function %s", node.getName());
            }

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
                checkState(window != null, "no resolved window for: %s", node);

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

            List<Expression> argumentValues = node.argumentValues();
            List<TypeDescriptorProvider> rawTypes = alignArgumentCollations(node, findCandidates(node.getName()), argumentValues, getCallArgumentTypes(argumentValues, context));
            // rawTypes may be shorter than the AST argument count when a `label.*`
            // entry is consumed by getCallArgumentTypes; bind to rawTypes.size() so
            // identity-binding of positional calls still indexes safely.
            boolean hasNamedArguments = node.hasNamedArguments();
            List<Integer> argumentBinding = hasNamedArguments
                    ? computeArgumentBinding(node)
                    : identityBinding(rawTypes.size());
            List<TypeDescriptorProvider> argumentTypes = argumentBinding.stream()
                    .map(rawTypes::get)
                    .collect(toImmutableList());

            if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                // Due to fact that the LISTAGG function is transformed out of pragmatic reasons
                // in a synthetic function call, the type expression of this function call is evaluated
                // explicitly here in order to make sure that it is a varchar.
                Expression expression = argumentValues.getFirst();
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

                if (e instanceof LanguageFunctionAnalysisException) {
                    // report the original reason for language function analysis errors
                    throw e;
                }

                // otherwise, it must have failed due to a missing function or other reason, so we report an error at the
                // current location

                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }

            if (node.getArguments().size() > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for function call %s()", function.signature().getName().functionName());
            }
            checkArgumentCollations(node, function, argumentBinding.stream()
                    .map(index -> argumentValues.get(index))
                    .collect(toImmutableList()));
            recordLambdaArgumentShapes(function, argumentBinding.stream()
                    .map(index -> argumentValues.get(index))
                    .collect(toImmutableList()));

            if (isAggregation) {
                // An aggregation can compare its arguments, for DISTINCT or for an order-dependent
                // result such as max, so a string argument needs a collation
                node.getArguments().stream()
                        .map(CallArgument::getValue)
                        .filter(argument -> !(argument instanceof LambdaExpression))
                        .forEach(this::requireCollation);
            }
            if (node.getOrderBy().isPresent()) {
                for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
                    Type sortKeyType = process(sortItem.getSortKey(), context);
                    requireCollation(sortItem.getSortKey());
                    if (!sortKeyType.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, node, "ORDER BY can only be applied to orderable types (actual: %s)", sortKeyType.getDisplayName());
                    }
                }
            }

            BoundSignature signature = function.signature();
            Optional<Collated> resultCollation = stringResultCollation(node, signature.getReturnType(), node.getArguments().stream()
                    .map(CallArgument::getValue)
                    .filter(argument -> !(argument instanceof LambdaExpression))
                    .collect(toImmutableList()));
            for (int i = 0; i < argumentTypes.size(); i++) {
                Expression expression = node.getArguments().get(argumentBinding.get(i)).getValue();
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
                    Type actualType = plannerContext.getTypeManager().getType(argumentTypes.get(i).getTypeDescriptor());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), function);
            argumentBindings.put(NodeRef.<Expression>of(node), argumentBinding);

            // must run after arguments are processed and labels are recorded
            if (context.isPatternRecognition() && isAggregation) {
                analyzePatternAggregation(node, function);
            }

            Type resultType = setStringResultType(node, signature.getReturnType(), resultCollation);
            recordResultShape(node, function, resultType);
            return resultType;
        }

        /// Sets a call's type: its declared result type, or for a string result whose collation
        /// derives from its arguments, the declared type in that collation.
        private Type setStringResultType(Expression node, Type returnType, Optional<Collated> resultCollation)
        {
            Type type = resultCollation
                    .map(collated -> relabelStringResult(node, returnType, collated))
                    .orElse(returnType);
            return setExpressionType(node, type);
        }

        /// The collation of a string function's result: a function declared over default-collation
        /// types returns its result in the combination of its string arguments' collations. Empty
        /// when the result keeps its declared type.
        private Optional<Collated> stringResultCollation(Expression node, Type returnType, List<Expression> arguments)
        {
            if (!(returnType instanceof VarcharType varcharType) || !varcharType.getCollation().isDefault()) {
                return Optional.empty();
            }
            List<Expression> stringArguments = arguments.stream()
                    .filter(argument -> getEffectiveType(argument) instanceof VarcharType)
                    .collect(toImmutableList());
            List<Collated> collated = stringArguments.stream()
                    .map(this::collated)
                    .flatMap(Optional::stream)
                    .collect(toImmutableList());
            if (collated.stream().allMatch(value -> value.derivation() != Derivation.NONE && value.collation().isDefault())) {
                return Optional.empty();
            }
            try {
                return Optional.of(CollationSemantics.combine(collated));
            }
            catch (CollationConflictException e) {
                throw collationMismatch(node, e);
            }
        }

        /// Records a string function result's collation and returns the result's type in it. The
        /// call is typed in the collation, while its resolved function returns the declared type;
        /// translation relabels the call's result to the collated type.
        private Type relabelStringResult(Expression node, Type returnType, Collated collated)
        {
            collationDerivations.put(NodeRef.of(node), new Leaf(collated));
            return ((VarcharType) returnType).withCollation(collated.collation());
        }

        /// Computes the binding from signature-position to AST argument index. Returns the
        /// identity binding for a call without named arguments; otherwise pairs each named
        /// argument with the position where its name appears in the function's declared
        /// parameter names, leaving positional arguments at their written positions.
        ///
        // TODO: name-to-position resolution properly belongs in the function binder / type
        //  inference engine. Doing it here means we walk the function search path twice —
        //  once for the binding, once for the type-based resolution. Lifting it into the
        //  binder is a much bigger change.
        private List<Integer> computeArgumentBinding(FunctionCall node)
        {
            // SQL name resolution picks the first path entry that has any candidate;
            // we stop iterating the path after that. Registration enforces that
            // overloads of the same name at the same arity place each named parameter
            // at the same position (see InternalFunctionBundle), so the first
            // arity-matching overload that fits is enough to build the binding.
            return computeArgumentBinding(node.getArguments(), findCandidates(node.getName()), 0, node, "function " + node.getName());
        }

        /// Computes the binding from declared-argument position to AST argument index for
        /// a call with named arguments. `candidates` are the overloads in scope for the
        /// callee; `receiverSlots` is the number of leading signature slots not visible to
        /// the caller (1 for an instance method's `self`, 0 otherwise). `subject` names the
        /// callee for diagnostics (for example `function foo` or `method bar`).
        private List<Integer> computeArgumentBinding(List<CallArgument> arguments, List<FunctionMetadata> candidates, int receiverSlots, Node errorNode, String subject)
        {
            int arity = arguments.size();
            int firstNamedArgument = findFirstNamedArgument(arguments);
            verifyNoDuplicateNames(arguments);

            Optional<FunctionMetadata> chosen = Optional.empty();
            for (FunctionMetadata candidate : candidates) {
                if (candidate.getSignature().isVariableArity()) {
                    // Variadic + named args is intentionally unsupported.
                    continue;
                }
                List<Signature.Argument> declared = callerVisibleArguments(candidate, receiverSlots);
                if (declared.size() == arity && satisfiesNamedArguments(arguments, firstNamedArgument, declared)) {
                    chosen = Optional.of(candidate);
                    break;
                }
            }

            if (chosen.isEmpty()) {
                Set<String> knownNames = new LinkedHashSet<>();
                for (FunctionMetadata candidate : candidates) {
                    if (candidate.getSignature().isVariableArity()) {
                        continue;
                    }
                    callerVisibleArguments(candidate, receiverSlots).forEach(argument -> argument.name().ifPresent(knownNames::add));
                }
                for (int i = firstNamedArgument; i < arity; i++) {
                    Identifier name = arguments.get(i).getName().orElseThrow();
                    if (!knownNames.contains(name.getValue())) {
                        throw semanticException(INVALID_FUNCTION_ARGUMENT, name, "No argument named %s for %s", name.getValue(), subject);
                    }
                }
                // Names are known but no overload accepts them — typically because a
                // name maps to a slot already supplied positionally. Surface that
                // specifically; otherwise fall through to a generic message.
                for (FunctionMetadata candidate : candidates) {
                    for (int i = firstNamedArgument; i < arity; i++) {
                        Identifier name = arguments.get(i).getName().orElseThrow();
                        OptionalInt declaredPosition = findArgumentPosition(callerVisibleArguments(candidate, receiverSlots), name.getValue());
                        if (declaredPosition.isPresent() && declaredPosition.orElseThrow() < firstNamedArgument) {
                            throw semanticException(
                                    INVALID_FUNCTION_ARGUMENT,
                                    name,
                                    "Named argument %s for %s refers to parameter position %s, which is already supplied positionally",
                                    name.getValue(),
                                    subject,
                                    declaredPosition.orElseThrow());
                        }
                    }
                }
                throw semanticException(INVALID_FUNCTION_ARGUMENT, errorNode, "No overload of %s accepts the given named arguments", subject);
            }

            List<Signature.Argument> chosenArguments = callerVisibleArguments(chosen.get(), receiverSlots);
            List<Integer> binding = new ArrayList<>(identityBinding(arity));
            for (int i = firstNamedArgument; i < arity; i++) {
                Identifier name = arguments.get(i).getName().orElseThrow();
                int signaturePosition = findArgumentPosition(chosenArguments, name.getValue()).orElseThrow();
                binding.set(signaturePosition, i);
            }
            return List.copyOf(binding);
        }

        /// Returns the declared arguments visible to the caller, dropping the leading
        /// `receiverSlots` entries that the call syntax supplies implicitly (an instance
        /// method's `self`).
        private static List<Signature.Argument> callerVisibleArguments(FunctionMetadata candidate, int receiverSlots)
        {
            List<Signature.Argument> arguments = candidate.getSignature().getArguments();
            return arguments.subList(receiverSlots, arguments.size());
        }

        private List<FunctionMetadata> findInstanceMethodCandidates(String methodName, Type receiverType)
        {
            String receiverBase = receiverType.getTypeDescriptor().getBase();
            return findMethodCandidates(methodName, candidate -> candidate.isInstanceMethod()
                    && candidate.getReceiverType().map(TypeTemplate::baseName).equals(Optional.of(receiverBase)));
        }

        private List<FunctionMetadata> findStaticMethodCandidates(String methodName, String receiverBase)
        {
            return findMethodCandidates(methodName, candidate -> !candidate.isInstanceMethod()
                    && candidate.getReceiverType().map(TypeTemplate::baseName).equals(Optional.of(receiverBase)));
        }

        /// Returns the method overloads named `methodName` from the first function-path
        /// entry that declares one matching `filter` (instance vs static and receiver
        /// type). This follows the same first-path-entry strategy as [#findCandidates]
        /// — and inherits the same approximation of [FunctionResolver]'s binding, which
        /// aggregates across the path: name binding only reads parameter names, and
        /// registration keeps each name at a stable position per name+arity, so the
        /// first matching entry's overloads are enough to build the binding.
        private List<FunctionMetadata> findMethodCandidates(String methodName, java.util.function.Predicate<FunctionMetadata> filter)
        {
            for (CatalogSchemaFunctionName candidateName : FunctionResolver.toPath(session, QualifiedName.of(methodName), accessControl)) {
                List<FunctionMetadata> matching = plannerContext.getMetadata().getFunctions(session, candidateName).stream()
                        .map(CatalogFunctionMetadata::functionMetadata)
                        .filter(filter)
                        .collect(toImmutableList());
                if (!matching.isEmpty()) {
                    return matching;
                }
            }
            return List.of();
        }

        /// Returns the index of the first named argument (or `arguments.size()` for
        /// a purely positional call). Throws if any positional argument appears after
        /// a named one, enforcing the SQL-spec "positional before named" rule.
        private static int findFirstNamedArgument(List<CallArgument> arguments)
        {
            int position = 0;
            while (position < arguments.size() && arguments.get(position).getName().isEmpty()) {
                position++;
            }
            for (int i = position; i < arguments.size(); i++) {
                if (arguments.get(i).getName().isEmpty()) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, arguments.get(i), "Positional arguments cannot follow named arguments");
                }
            }
            return position;
        }

        private static void verifyNoDuplicateNames(List<CallArgument> arguments)
        {
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < arguments.size(); i++) {
                if (arguments.get(i).getName().isEmpty()) {
                    continue;
                }
                Identifier name = arguments.get(i).getName().orElseThrow();
                if (!seen.add(name.getValue())) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, name, "Duplicate named argument: %s", name.getValue());
                }
            }
        }

        private static boolean satisfiesNamedArguments(List<CallArgument> actualArguments, int firstNamedArgument, List<Signature.Argument> formalArguments)
        {
            for (int i = firstNamedArgument; i < actualArguments.size(); i++) {
                String name = actualArguments.get(i).getName().orElseThrow().getValue();
                OptionalInt declaredPosition = findArgumentPosition(formalArguments, name);
                if (declaredPosition.isEmpty() || declaredPosition.orElseThrow() < firstNamedArgument) {
                    return false;
                }
            }
            return true;
        }

        private List<FunctionMetadata> findCandidates(QualifiedName name)
        {
            for (CatalogSchemaFunctionName candidateName : FunctionResolver.toPath(session, name, accessControl)) {
                Collection<CatalogFunctionMetadata> candidates = plannerContext.getMetadata().getFunctions(session, candidateName);
                if (!candidates.isEmpty()) {
                    return candidates.stream()
                            .map(CatalogFunctionMetadata::functionMetadata)
                            .collect(toImmutableList());
                }
            }
            return List.of();
        }

        private static OptionalInt findArgumentPosition(List<Signature.Argument> arguments, String name)
        {
            for (int i = 0; i < arguments.size(); i++) {
                if (arguments.get(i).name().equals(Optional.of(name))) {
                    return OptionalInt.of(i);
                }
            }
            return OptionalInt.empty();
        }

        private static List<Integer> identityBinding(int arity)
        {
            ImmutableList.Builder<Integer> result = ImmutableList.builderWithExpectedSize(arity);
            for (int i = 0; i < arity; i++) {
                result.add(i);
            }
            return result.build();
        }

        /// Returns the argument values reordered into declared-signature order, as given
        /// by `binding` (the identity for a purely positional call).
        private static List<Expression> orderedArgumentValues(List<CallArgument> arguments, List<Integer> binding)
        {
            return binding.stream()
                    .map(arguments::get)
                    .map(CallArgument::getValue)
                    .collect(toImmutableList());
        }

        private Optional<Type> tryResolveAsInstanceMethod(FunctionCall node, Context context)
        {
            QualifiedName name = node.getName();
            if (name.getParts().size() != 2) {
                return Optional.empty();
            }
            if (node.isDistinct()
                    || node.getFilter().isPresent()
                    || node.getOrderBy().isPresent()
                    || node.getWindow().isPresent()
                    || node.getProcessingMode().isPresent()
                    || node.getNullTreatment().isPresent()) {
                return Optional.empty();
            }
            if (context.isPatternRecognition() || context.isInWindow()) {
                return Optional.empty();
            }

            Identifier receiver = name.getOriginalParts().get(0);
            Identifier method = name.getOriginalParts().get(1);

            // Method-call interpretation only applies when the receiver resolves
            // as a field in the current scope. tryResolveField has no side effects
            // so a non-match leaves the analyzer state untouched.
            Optional<ResolvedField> resolvedReceiver = context.getScope()
                    .tryResolveField(receiver, QualifiedName.of(receiver.getValue()));
            if (resolvedReceiver.isEmpty()) {
                return Optional.empty();
            }
            Type receiverType = resolvedReceiver.get().getField().getType();

            List<CallArgument> arguments = node.getArguments();
            List<Integer> binding;
            if (node.hasNamedArguments()) {
                List<FunctionMetadata> candidates = findInstanceMethodCandidates(method.getValue(), receiverType);
                if (candidates.isEmpty()) {
                    // No instance method of this name on the receiver type; let ordinary
                    // function resolution handle (and reject) the call.
                    return Optional.empty();
                }
                binding = computeArgumentBinding(arguments, candidates, 1, node, "method " + method.getValue());
            }
            else {
                binding = identityBinding(arguments.size());
            }
            List<Expression> argumentValues = orderedArgumentValues(arguments, binding);

            MethodResolution resolution;
            try {
                resolution = resolveInstanceMethodCall(receiverType, method.getValue(), argumentValues, context);
            }
            catch (TrinoException e) {
                return Optional.empty();
            }

            // Commit to method-call interpretation: record the receiver field reference.
            process(receiver, context);

            Type result = analyzeInstanceMethodInvocation(node, receiver, receiverType, method.getValue(), argumentValues, resolution, context);
            methodCallReceivers.put(NodeRef.of(node), receiver);
            argumentBindings.put(NodeRef.<Expression>of(node), binding);
            return Optional.of(result);
        }

        private Type visitMethodCall(MethodCall node, Context context)
        {
            Optional<Type> asJsonItemMethod = tryAnalyzeJsonItemMethodAccessor(node, context);
            if (asJsonItemMethod.isPresent()) {
                return setExpressionType(node, asJsonItemMethod.get());
            }

            Type receiverType = process(node.getReceiver(), context);
            String methodName = node.getMethod().getValue();

            List<CallArgument> arguments = node.getArguments();
            List<Integer> binding;
            if (node.hasNamedArguments()) {
                List<FunctionMetadata> candidates = findInstanceMethodCandidates(methodName, receiverType);
                // With no instance method of this name on the receiver type, bind positionally so
                // resolution reports method-not-found rather than a misleading "No argument named ...".
                binding = candidates.isEmpty()
                        ? identityBinding(arguments.size())
                        : computeArgumentBinding(arguments, candidates, 1, node, "method " + methodName);
            }
            else {
                binding = identityBinding(arguments.size());
            }
            List<Expression> argumentValues = orderedArgumentValues(arguments, binding);

            MethodResolution resolution;
            try {
                resolution = resolveInstanceMethodCall(receiverType, methodName, argumentValues, context);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }

            argumentBindings.put(NodeRef.<Expression>of(node), binding);
            return analyzeInstanceMethodInvocation(node, node.getReceiver(), receiverType, methodName, argumentValues, resolution, context);
        }

        private MethodResolution resolveInstanceMethodCall(Type receiverType, String methodName, List<Expression> arguments, Context context)
        {
            List<TypeDescriptorProvider> argumentTypes = ImmutableList.<TypeDescriptorProvider>builder()
                    .add(new TypeDescriptorProvider(receiverType.getTypeDescriptor()))
                    .addAll(getCallArgumentTypes(arguments, context))
                    .build();
            ResolvedFunction function = functionResolver.resolveInstanceMethod(
                    session,
                    receiverType.getTypeDescriptor(),
                    QualifiedName.of(methodName),
                    argumentTypes,
                    accessControl);
            return new MethodResolution(function, argumentTypes);
        }

        private Type analyzeInstanceMethodInvocation(
                Expression node,
                Expression receiver,
                Type receiverType,
                String methodName,
                List<Expression> arguments,
                MethodResolution resolution,
                Context context)
        {
            if (arguments.size() + 1 > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for method call .%s()", methodName);
            }

            checkArgumentCollations(node, resolution.function(), ImmutableList.<Expression>builder()
                    .add(receiver)
                    .addAll(arguments)
                    .build());
            recordLambdaArgumentShapes(resolution.function(), ImmutableList.<Expression>builder()
                    .add(receiver)
                    .addAll(arguments)
                    .build());
            BoundSignature signature = resolution.function().signature();
            Optional<Collated> resultCollation = stringResultCollation(node, signature.getReturnType(), ImmutableList.<Expression>builder()
                    .add(receiver)
                    .addAll(arguments.stream().filter(argument -> !(argument instanceof LambdaExpression)).iterator())
                    .build());
            Type expectedReceiverType = signature.getArgumentTypes().getFirst();
            coerceType(receiver, receiverType, expectedReceiverType, format("Method .%s receiver", methodName));
            // Slot 0 of the signature is the receiver (self), so user-visible argument i maps to signature slot i + 1.
            for (int i = 0; i < arguments.size(); i++) {
                Expression expression = arguments.get(i);
                Type expectedType = signature.getArgumentTypes().get(i + 1);
                if (resolution.argumentTypes().get(i + 1).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, context.expectingLambda(expectedFunctionType.getArgumentTypes()));
                }
                else {
                    Type actualType = plannerContext.getTypeManager().getType(resolution.argumentTypes().get(i + 1).getTypeDescriptor());
                    coerceType(expression, actualType, expectedType, format("Method .%s argument %d", methodName, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), resolution.function());
            Type resultType = setStringResultType(node, signature.getReturnType(), resultCollation);
            recordResultShape(node, resolution.function(), resultType);
            return resultType;
        }

        private record MethodResolution(ResolvedFunction function, List<TypeDescriptorProvider> argumentTypes) {}

        private Type visitStaticMethodCall(StaticMethodCall node, Context context)
        {
            QualifiedName receiver = node.getType();
            if (receiver.getParts().size() != 1 || !plannerContext.getTypeManager().isTypeRegistered(receiver.getSuffix())) {
                throw semanticException(TYPE_NOT_FOUND, node, "Unknown type: %s", receiver);
            }
            TypeDescriptor receiverDescriptor = new TypeDescriptor(receiver.getSuffix());
            String methodName = node.getMethod().getValue();

            List<CallArgument> arguments = node.getArguments();
            List<Integer> binding;
            if (node.hasNamedArguments()) {
                List<FunctionMetadata> candidates = findStaticMethodCandidates(methodName, receiverDescriptor.getBase());
                // With no static method of this name on the receiver type, bind positionally so
                // resolution reports method-not-found rather than a misleading "No argument named ...".
                binding = candidates.isEmpty()
                        ? identityBinding(arguments.size())
                        : computeArgumentBinding(arguments, candidates, 0, node, "static method " + receiver + "::" + methodName);
            }
            else {
                binding = identityBinding(arguments.size());
            }
            List<Expression> argumentValues = orderedArgumentValues(arguments, binding);

            List<TypeDescriptorProvider> argumentTypes = getCallArgumentTypes(argumentValues, context);

            ResolvedFunction function;
            try {
                function = functionResolver.resolveStaticMethod(
                        session,
                        receiverDescriptor,
                        QualifiedName.of(methodName),
                        argumentTypes,
                        accessControl);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }

            if (argumentValues.size() > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for static method call %s::%s()", receiver, methodName);
            }

            BoundSignature signature = function.signature();
            Optional<Collated> resultCollation = stringResultCollation(node, signature.getReturnType(), argumentValues.stream()
                    .filter(argument -> !(argument instanceof LambdaExpression))
                    .collect(toImmutableList()));
            for (int i = 0; i < argumentTypes.size(); i++) {
                Expression expression = argumentValues.get(i);
                Type expectedType = signature.getArgumentTypes().get(i);
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, context.expectingLambda(expectedFunctionType.getArgumentTypes()));
                }
                else {
                    Type actualType = plannerContext.getTypeManager().getType(argumentTypes.get(i).getTypeDescriptor());
                    coerceType(expression, actualType, expectedType, format("Static method %s::%s argument %d", receiver, methodName, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), function);
            argumentBindings.put(NodeRef.<Expression>of(node), binding);
            return setStringResultType(node, signature.getReturnType(), resultCollation);
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
                    requireCollation(expression);
                    Type type = getExpressionType(expression);
                    if (!type.isComparable()) {
                        throw semanticException(TYPE_MISMATCH, expression, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                    if (!type.isOrderable()) {
                        // TODO this is not a hard requirement, just the implication of how we do partitioning right now, using PagesIndex and iterating over ordered values
                        // to find partition boundaries.
                        throw semanticException(TYPE_MISMATCH, expression, "%s is not orderable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }
            }

            if (!window.isOrderByInherited()) {
                for (SortItem sortItem : getSortItemsFromOrderBy(window.getOrderBy())) {
                    process(sortItem.getSortKey(), context);
                    requireCollation(sortItem.getSortKey());
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
                    if (frame.getExclusion() != WindowFrame.Exclusion.NO_OTHERS) {
                        throw semanticException(INVALID_WINDOW_FRAME, frame, "Frame exclusion is not allowed with pattern recognition");
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
                FrameBound.Type endType = frame.getEnd().map(FrameBound::getType).orElse(CURRENT_ROW);
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
            // TODO: Support NUMBER as the sort key type and the frame bound type of a RANGE frame with offsets
            boolean numericSortKey = isNumericType(sortKeyType) && !sortKeyType.equals(NUMBER);
            if (!numericSortKey && !isDateTimeType(sortKeyType)) {
                throw semanticException(TYPE_MISMATCH, sortKey, "Window frame of type RANGE PRECEDING or FOLLOWING requires that sort item type be numeric, datetime or interval (actual: %s)", sortKeyType);
            }

            Type offsetValueType = process(offsetValue, context);

            if (numericSortKey) {
                if (!isNumericType(offsetValueType) || offsetValueType.equals(NUMBER)) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
            }
            else { // isDateTimeType(sortKeyType)
                if (!(offsetValueType instanceof IntervalDayTimeType) && !(offsetValueType instanceof IntervalYearMonthType)) {
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
                function = plannerContext.getMetadata().resolveOperator(charVarcharCoercion, operatorType, ImmutableList.of(sortKeyType, offsetValueType));
            }
            catch (TrinoException e) {
                ErrorCode errorCode = e.getErrorCode();
                if (errorCode.equals(OPERATOR_NOT_FOUND.toErrorCode())) {
                    throw semanticException(TYPE_MISMATCH, offsetValue, "Window frame RANGE value type (%s) not compatible with sort item type (%s)", offsetValueType, sortKeyType);
                }
                throw e;
            }
            BoundSignature signature = function.signature();
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

        private Type visitWindowOperation(WindowOperation node, Context context)
        {
            ResolvedWindow window = getResolvedWindow.apply(node);
            checkState(window != null, "no resolved window for: %s", node);

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

        public List<TypeDescriptorProvider> getCallArgumentTypes(List<Expression> arguments, Context context)
        {
            ImmutableList.Builder<TypeDescriptorProvider> argumentTypesBuilder = ImmutableList.builder();
            for (Expression argument : arguments) {
                if (argument instanceof LambdaExpression) {
                    argumentTypesBuilder.add(new TypeDescriptorProvider(
                            types -> {
                                ExpressionAnalyzer innerExpressionAnalyzer = new ExpressionAnalyzer(
                                        plannerContext,
                                        accessControl,
                                        statementAnalyzerFactory,
                                        session,
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
                                return innerExpressionAnalyzer.analyze(argument, baseScope, context.expectingLambda(types)).getTypeDescriptor();
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
                        argumentTypesBuilder.add(new TypeDescriptorProvider(process(argument, context).getTypeDescriptor()));
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
                Expression argument = node.getArguments().getFirst().getValue();
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
                    node.getArguments().getFirst().getValue(),
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
                    node.getArguments().getFirst().getValue(),
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
                offset = (int) ((LongLiteral) node.getArguments().get(1).getValue()).getParsedValue();
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
                if (!(node.getArguments().get(1).getValue() instanceof LongLiteral)) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "%s pattern recognition navigation function requires a number as the second argument", node.getName());
                }
                long offset = ((LongLiteral) node.getArguments().get(1).getValue()).getParsedValue();
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
            List<FunctionCall> nestedNavigationFunctions = extractExpressions(ImmutableList.of(node.getArguments().getFirst().getValue()), FunctionCall.class).stream()
                    .filter(this::isPatternNavigationFunction)
                    .collect(toImmutableList());
            if (!nestedNavigationFunctions.isEmpty()) {
                if (name.equalsIgnoreCase("FIRST") || name.equalsIgnoreCase("LAST")) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nestedNavigationFunctions.getFirst(),
                            "Cannot nest %s pattern navigation function inside %s pattern navigation function",
                            nestedNavigationFunctions.getFirst().getName(),
                            name);
                }
                if (nestedNavigationFunctions.size() > 1) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nestedNavigationFunctions.get(1),
                            "Cannot nest multiple pattern navigation functions inside %s pattern navigation function",
                            name);
                }
                FunctionCall nested = getOnlyElement(nestedNavigationFunctions);
                String nestedName = nested.getName().getSuffix();
                if (nestedName.equalsIgnoreCase("PREV") || nestedName.equalsIgnoreCase("NEXT")) {
                    throw semanticException(
                            INVALID_NAVIGATION_NESTING,
                            nested,
                            "Cannot nest %s pattern navigation function inside %s pattern navigation function",
                            nestedName,
                            name);
                }
                if (nested != node.getArguments().getFirst().getValue()) {
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
            List<Expression> argumentValues = node.argumentValues();
            Set<Optional<String>> referenceLabels = extractExpressions(argumentValues, Expression.class).stream()
                    .map(child -> labels.get(NodeRef.of(child)))
                    .filter(Objects::nonNull)
                    .collect(toImmutableSet());

            Set<Optional<String>> classifierLabels = extractExpressions(ImmutableList.of(argumentValues.get(argumentIndex)), FunctionCall.class).stream()
                    .filter(this::isClassifierFunction)
                    .map(functionCall -> functionCall.argumentValues().stream()
                            .findFirst()
                            .map(value -> label((Identifier) value)))
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
                        "All labels and classifiers inside the call to '%s' must match",
                        name);
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

            List<Expression> argumentValues = node.argumentValues();

            List<FunctionCall> matchNumberCalls = extractExpressions(argumentValues, FunctionCall.class).stream()
                    .filter(this::isMatchNumberFunction)
                    .collect(toImmutableList());

            List<FunctionCall> classifierCalls = extractExpressions(argumentValues, FunctionCall.class).stream()
                    .filter(this::isClassifierFunction)
                    .collect(toImmutableList());

            patternRecognitionInputs.add(new PatternInputAnalysis(
                    node,
                    new AggregationDescriptor(
                            function,
                            argumentValues,
                            mapProcessingMode(node.getProcessingMode()),
                            labels,
                            matchNumberCalls,
                            classifierCalls)));
        }

        private void checkNoNestedAggregations(FunctionCall node)
        {
            extractExpressions(node.argumentValues(), FunctionCall.class).stream()
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
            extractExpressions(node.argumentValues(), FunctionCall.class).stream()
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

        private Type visitAtTimeZone(AtTimeZone node, Context context)
        {
            Type valueType = process(node.getValue(), context);
            process(node.getTimeZone(), context);
            Type resultType = switch (valueType) {
                case TimeType type -> createTimeWithTimeZoneType(type.getPrecision());
                case TimestampType type -> createTimestampWithTimeZoneType(type.getPrecision());
                case TimeWithTimeZoneType _, TimestampWithTimeZoneType _ -> valueType;
                default -> throw semanticException(TYPE_MISMATCH, node.getValue(), "Type of value must be a time or timestamp with or without time zone (actual %s)", valueType);
            };

            return setExpressionType(node, resultType);
        }

        private Type visitCollateExpression(CollateExpression node, Context context)
        {
            Type valueType = process(node.getValue(), context);
            VarcharType varcharType;
            if (valueType instanceof VarcharType type) {
                varcharType = type;
            }
            else if (valueType.equals(UNKNOWN)) {
                varcharType = VARCHAR;
                addOrReplaceExpressionCoercion(node.getValue(), varcharType);
            }
            else {
                throw semanticException(TYPE_MISMATCH, node.getValue(), "COLLATE requires a varchar value (actual %s)", valueType);
            }
            String name = TypeDescriptorTranslator.canonicalize(node.getCollation());
            Collation collation = collationRegistry().findCollation(name)
                    .orElseThrow(() -> semanticException(COLLATION_NOT_FOUND, node.getCollation(), "Collation not found: %s", name));
            collationDerivations.put(NodeRef.of(node), new Leaf(Collated.explicit(collation)));
            return setExpressionType(node, varcharType.withCollation(collation));
        }

        private Type visitCollationFor(CollationFor node, Context context)
        {
            Type valueType = process(node.getValue(), context);
            if (!(valueType instanceof VarcharType)) {
                throw semanticException(TYPE_MISMATCH, node.getValue(), "COLLATION FOR requires a varchar value (actual %s)", valueType);
            }
            // a value of derivation none has no collation
            determineCollation(node, ImmutableList.of(node.getValue()));
            return setExpressionType(node, VARCHAR);
        }

        /// The derivations of the strings an expression's value contains, seen through its
        /// effective type.
        private CollationShape shape(Expression expression)
        {
            Type type = getEffectiveType(expression);
            CollationShape recorded = collationDerivations.get(NodeRef.of(expression));
            if (recorded == null) {
                return CollationShapes.implicit(type);
            }
            // a relabeled operand keeps its derivations and has the collations it was relabeled to
            return CollationShapes.reconcile(recorded, type);
        }

        /// The collation and derivation of a character string expression, or empty for other types.
        private Optional<Collated> collated(Expression expression)
        {
            if (shape(expression) instanceof Leaf(Collated collated)) {
                return Optional.of(collated);
            }
            return Optional.empty();
        }

        private Type getEffectiveType(Expression expression)
        {
            Type coercion = expressionCoercions.get(NodeRef.of(expression));
            if (coercion != null) {
                return coercion;
            }
            return getExpressionType(expression);
        }

        /// Applies collation determination to the operands of a comparison, at each string
        /// position of composite operands, failing when their collations conflict, and relabels
        /// operands to the determined collations. Returns the operand types after relabeling.
        private List<Type> determineCollation(Expression anchor, List<Expression> operands)
        {
            Optional<CollationShape> determined = determinedShape(anchor, operands);
            if (determined.isEmpty()) {
                return operands.stream().map(this::getEffectiveType).collect(toImmutableList());
            }
            return relabel(operands, determined.get());
        }

        /// The collation determined for comparing character string operands, or empty when no
        /// operand is a collated string, so that no relabeling is needed.
        private Optional<Collation> determinedCollation(Expression anchor, List<Expression> operands)
        {
            return determinedShape(anchor, operands)
                    .filter(Leaf.class::isInstance)
                    .map(shape -> ((Leaf) shape).collated().collation());
        }

        private Optional<CollationShape> determinedShape(Expression anchor, List<Expression> operands)
        {
            List<CollationShape> shapes = operands.stream()
                    .map(this::shape)
                    .collect(toImmutableList());
            boolean trivial = true;
            for (int i = 0; i < operands.size(); i++) {
                trivial &= CollationShapes.isTrivial(shapes.get(i), getEffectiveType(operands.get(i)));
            }
            if (trivial) {
                return Optional.empty();
            }
            CollationShape determined;
            try {
                determined = CollationSemantics.determineShapes(shapes);
            }
            catch (CollationConflictException e) {
                throw collationMismatch(anchor, e);
            }
            rejectCollatedWithChar(anchor, operands.stream().map(this::getEffectiveType).collect(toImmutableList()));
            return Optional.of(determined);
        }

        /// Fails when collated strings would be compared with a char. A char has no collation, and
        /// comparing it with a collated string would compare code points: `'a' COLLATE unicode_ci`
        /// would not equal `CAST('A' AS char(1))`.
        private void rejectCollatedWithChar(Node anchor, List<Type> operandTypes)
        {
            boolean collated = operandTypes.stream().anyMatch(type -> hasNonDefaultCollation(type));
            if (collated && operandTypes.stream().anyMatch(type -> containsChar(type))) {
                throw semanticException(COLLATION_MISMATCH, anchor, "A collated string cannot be compared with a char; cast the char to varchar");
            }
        }

        private static boolean containsChar(Type type)
        {
            return type instanceof CharType || type.getTypeParameters().stream().anyMatch(parameter -> containsChar(parameter));
        }

        private static Type withCollation(Type type, Collation collation)
        {
            if (type instanceof VarcharType varcharType) {
                return varcharType.withCollation(collation);
            }
            return type;
        }

        /// Applies the result of data type combinations to operands that combine into one value,
        /// at each string position, relabels operands to the combined collations, and returns the
        /// operand types after relabeling together with the shape of the result.
        private CombinedCollation combineCollations(Expression anchor, List<Expression> operands)
        {
            List<CollationShape> shapes = operands.stream()
                    .map(this::shape)
                    .collect(toImmutableList());
            CollationShape combined;
            try {
                combined = CollationSemantics.combineShapes(shapes);
            }
            catch (CollationConflictException e) {
                throw collationMismatch(anchor, e);
            }
            if (combined == CollationShape.Opaque.OPAQUE) {
                return new CombinedCollation(operands.stream().map(this::getEffectiveType).collect(toImmutableList()), Optional.empty());
            }
            // Operands whose implicit collations differ combine into the default-collation type with
            // derivation none, which prevents comparing the result
            return new CombinedCollation(relabel(operands, combined), Optional.of(combined));
        }

        private List<Type> relabel(List<Expression> operands, CollationShape shape)
        {
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (Expression operand : operands) {
                Type type = getEffectiveType(operand);
                Type relabeled = CollationShapes.applyTo(type, shape, plannerContext.getTypeManager());
                if (relabeled != type) {
                    addOrReplaceExpressionCoercion(operand, relabeled);
                }
                types.add(relabeled);
            }
            return types.build();
        }

        private TrinoException collationMismatch(Expression anchor, CollationConflictException e)
        {
            if (e.derivation() == Derivation.EXPLICIT) {
                return semanticException(COLLATION_MISMATCH, anchor, "Explicit collations %s conflict", e.collations());
            }
            return semanticException(COLLATION_MISMATCH, anchor, "Collations %s conflict; use COLLATE to choose a collation", e.collations());
        }

        private record CombinedCollation(List<Type> types, Optional<CollationShape> shape) {}

        /// Records the shape of a node's value, when it differs from the implicit shape of its type.
        private void recordShape(Expression node, CollationShape shape, Type type)
        {
            if (!shape.equals(CollationShapes.implicit(type))) {
                collationDerivations.put(NodeRef.of(node), shape);
            }
        }

        /// Aligns the collations of a call's arguments before the function is resolved, so that a
        /// declaration over a type variable unifies arguments whose strings would combine into one
        /// collation, such as a collated array and a literal element. The strings bound to each
        /// type variable combine separately, so a map's keys and values keep their own collations.
        /// Strings whose implicit collations differ are left as they are, and fail where they are
        /// compared. Only positions that every candidate declaration binds to the same variable are
        /// aligned, so an overload is never chosen by relabeling.
        private List<TypeDescriptorProvider> alignArgumentCollations(Expression node, List<FunctionMetadata> candidates, List<Expression> arguments, List<TypeDescriptorProvider> argumentTypes)
        {
            if (argumentTypes.size() != arguments.size() || candidates.isEmpty()) {
                // a label.* argument is consumed; such calls take no strings to align
                return argumentTypes;
            }
            List<Expression> values = arguments.stream()
                    .filter(argument -> !(argument instanceof LambdaExpression))
                    .collect(toImmutableList());
            if (values.size() < 2 || values.stream().allMatch(value -> CollationShapes.isTrivial(shape(value), getEffectiveType(value)) && !hasCoercible(shape(value)))) {
                return argumentTypes;
            }
            // the shape template of each argument, shared by every candidate: the variable bound at
            // each string position, or empty when the candidates disagree
            // only declarations whose formal types can accept the arguments' structures take part
            List<Type> types = arguments.stream()
                    .map(argument -> argument instanceof LambdaExpression ? null : getEffectiveType(argument))
                    .collect(toList());
            List<FunctionMetadata> applicable = candidates.stream()
                    .filter(candidate -> fits(candidate.getSignature(), types))
                    .collect(toImmutableList());
            List<Optional<VariableTemplate>> templates = new ArrayList<>();
            for (int i = 0; i < arguments.size(); i++) {
                templates.add(sharedTemplate(applicable, i, arguments.size()));
            }
            Map<String, List<Collated>> bound = new LinkedHashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                if (arguments.get(i) instanceof LambdaExpression || templates.get(i).isEmpty()) {
                    continue;
                }
                bindLeaves(templates.get(i).get(), shape(arguments.get(i)), bound);
            }
            Map<String, Collation> targets = new LinkedHashMap<>();
            for (Map.Entry<String, List<Collated>> entry : bound.entrySet()) {
                if (entry.getValue().size() < 2) {
                    continue;
                }
                Collated combined;
                try {
                    combined = CollationSemantics.combine(entry.getValue());
                }
                catch (CollationConflictException e) {
                    throw collationMismatch(node, e);
                }
                if (combined.derivation() != Derivation.NONE) {
                    targets.put(entry.getKey(), combined.collation());
                }
            }
            if (targets.isEmpty()) {
                return argumentTypes;
            }
            ImmutableList.Builder<TypeDescriptorProvider> aligned = ImmutableList.builder();
            for (int i = 0; i < arguments.size(); i++) {
                Expression argument = arguments.get(i);
                if (argument instanceof LambdaExpression || templates.get(i).isEmpty()) {
                    aligned.add(argumentTypes.get(i));
                    continue;
                }
                Type type = getEffectiveType(argument);
                Type relabeled = CollationShapes.applyTo(type, targetShape(templates.get(i).get(), shape(argument), targets), plannerContext.getTypeManager());
                if (relabeled != type) {
                    addOrReplaceExpressionCoercion(argument, relabeled);
                }
                aligned.add(new TypeDescriptorProvider(relabeled.getTypeDescriptor()));
            }
            return aligned.build();
        }

        // the structure of a formal argument type that binds strings: a type variable, a composite
        // of such, or a position that binds none
        private sealed interface VariableTemplate
        {
            record Variable(String name)
                    implements VariableTemplate {}

            record Composite(String base, List<VariableTemplate> parameters)
                    implements VariableTemplate {}

            record Unbound()
                    implements VariableTemplate {}
        }

        // whether each formal can accept the argument's type structurally: a type variable accepts
        // any type, and a type application accepts a type of the same base whose nested types fit
        private static boolean fits(Signature signature, List<Type> types)
        {
            List<TypeTemplate> formals = signature.getArgumentTypes();
            if (formals.isEmpty() || (!signature.isVariableArity() && formals.size() != types.size())) {
                return false;
            }
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                if (type != null && !fits(formals.get(Math.min(i, formals.size() - 1)), type)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean fits(TypeTemplate formal, Type type)
        {
            return switch (formal) {
                case TypeTemplate.TypeVariable _ -> true;
                case TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters) -> {
                    if (type.equals(UNKNOWN)) {
                        yield true;
                    }
                    if (!base.equalsIgnoreCase(type.getTypeDescriptor().getBase())) {
                        yield false;
                    }
                    List<TypeTemplate> nested = parameters.stream()
                            .filter(TemplateParameter.TypeArgument.class::isInstance)
                            .map(parameter -> ((TemplateParameter.TypeArgument) parameter).type())
                            .collect(toImmutableList());
                    List<Type> nestedTypes = type.getTypeParameters();
                    if (nested.isEmpty() || nested.size() != nestedTypes.size()) {
                        yield true;
                    }
                    for (int i = 0; i < nested.size(); i++) {
                        if (!fits(nested.get(i), nestedTypes.get(i))) {
                            yield false;
                        }
                    }
                    yield true;
                }
            };
        }

        private static Optional<VariableTemplate> sharedTemplate(List<FunctionMetadata> candidates, int argument, int arity)
        {
            Optional<VariableTemplate> shared = Optional.empty();
            for (FunctionMetadata candidate : candidates) {
                List<TypeTemplate> formals = candidate.getSignature().getArgumentTypes();
                if (formals.isEmpty() || (!candidate.getSignature().isVariableArity() && formals.size() != arity)) {
                    continue;
                }
                VariableTemplate template = variableTemplate(formals.get(Math.min(argument, formals.size() - 1)));
                if (shared.isPresent() && !shared.get().equals(template)) {
                    return Optional.empty();
                }
                shared = Optional.of(template);
            }
            return shared;
        }

        private static VariableTemplate variableTemplate(TypeTemplate formal)
        {
            return switch (formal) {
                case TypeTemplate.TypeVariable(String name) -> new VariableTemplate.Variable(name);
                case TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters) -> {
                    List<VariableTemplate> nested = parameters.stream()
                            .filter(TemplateParameter.TypeArgument.class::isInstance)
                            .map(parameter -> variableTemplate(((TemplateParameter.TypeArgument) parameter).type()))
                            .collect(toImmutableList());
                    yield nested.stream().allMatch(VariableTemplate.Unbound.class::isInstance)
                            ? new VariableTemplate.Unbound()
                            : new VariableTemplate.Composite(base, nested);
                }
            };
        }

        // the leaves of an argument's shape at each variable of its template
        private static void bindLeaves(VariableTemplate template, CollationShape shape, Map<String, List<Collated>> bound)
        {
            switch (template) {
                case VariableTemplate.Variable(String name) -> bound.computeIfAbsent(name, _ -> new ArrayList<>()).addAll(leaves(shape));
                case VariableTemplate.Composite(String _, List<VariableTemplate> parameters) -> {
                    List<CollationShape> nested = nestedShapes(shape);
                    if (nested.size() == parameters.size()) {
                        for (int i = 0; i < parameters.size(); i++) {
                            bindLeaves(parameters.get(i), nested.get(i), bound);
                        }
                    }
                }
                case VariableTemplate.Unbound _ -> {}
            }
        }

        // the shape with the strings bound to each aligned variable set to its collation
        private static CollationShape targetShape(VariableTemplate template, CollationShape shape, Map<String, Collation> targets)
        {
            return switch (template) {
                case VariableTemplate.Variable(String name) -> {
                    Collation target = targets.get(name);
                    yield target == null ? shape : uniform(shape, new Leaf(Collated.implicit(target)));
                }
                case VariableTemplate.Composite(String _, List<VariableTemplate> parameters) -> {
                    List<CollationShape> nested = nestedShapes(shape);
                    if (nested.size() != parameters.size()) {
                        yield shape;
                    }
                    List<CollationShape> relabeled = new ArrayList<>();
                    for (int i = 0; i < parameters.size(); i++) {
                        relabeled.add(targetShape(parameters.get(i), nested.get(i), targets));
                    }
                    yield switch (shape) {
                        case ArrayShape _ -> new ArrayShape(relabeled.getFirst());
                        case MapShape _ -> new MapShape(relabeled.get(0), relabeled.get(1));
                        case RowShape _ -> new RowShape(relabeled);
                        default -> shape;
                    };
                }
                case VariableTemplate.Unbound _ -> shape;
            };
        }

        private static List<CollationShape> nestedShapes(CollationShape shape)
        {
            return switch (shape) {
                case ArrayShape(CollationShape element) -> ImmutableList.of(element);
                case MapShape(CollationShape key, CollationShape value) -> ImmutableList.of(key, value);
                case RowShape(List<CollationShape> fields) -> fields;
                default -> ImmutableList.of();
            };
        }

        // the type with every string it contains in the target leaf's collation
        private Type relabelStrings(Type type, CollationShape target)
        {
            return CollationShapes.applyTo(type, uniform(CollationShapes.implicit(type), target), plannerContext.getTypeManager());
        }

        private static CollationShape uniform(CollationShape shape, CollationShape leaf)
        {
            return switch (shape) {
                case Leaf _ -> leaf;
                case ArrayShape(CollationShape element) -> new ArrayShape(uniform(element, leaf));
                case MapShape(CollationShape key, CollationShape value) -> new MapShape(uniform(key, leaf), uniform(value, leaf));
                case RowShape(List<CollationShape> fields) -> new RowShape(fields.stream().map(field -> uniform(field, leaf)).collect(toImmutableList()));
                case CollationShape.Opaque opaque -> opaque;
            };
        }

        private static boolean hasCoercible(CollationShape shape)
        {
            return leaves(shape).stream().anyMatch(collated -> collated.derivation() == Derivation.COERCIBLE);
        }

        private static List<Collated> leaves(CollationShape shape)
        {
            return switch (shape) {
                case Leaf(Collated collated) -> ImmutableList.of(collated);
                case ArrayShape(CollationShape element) -> leaves(element);
                case MapShape(CollationShape key, CollationShape value) -> ImmutableList.<Collated>builder().addAll(leaves(key)).addAll(leaves(value)).build();
                case RowShape(List<CollationShape> fields) -> fields.stream().flatMap(field -> leaves(field).stream()).collect(toImmutableList());
                case CollationShape.Opaque _ -> ImmutableList.of();
            };
        }

        /// Checks the collations of a resolved call's arguments. A function that compares values
        /// of a type variable, such as `contains` or `greatest`, compares the strings they contain,
        /// so those strings need a collation. A string function declared over default-collation
        /// strings that searches or compares its arguments' contents cannot apply another
        /// collation, so it does not accept a non-default one.
        private void checkArgumentCollations(Expression node, ResolvedFunction function, List<Expression> arguments)
        {
            List<Expression> values = arguments.stream()
                    .filter(argument -> !(argument instanceof LambdaExpression))
                    .collect(toImmutableList());
            if (values.stream().allMatch(value -> CollationShapes.isTrivial(shape(value), getEffectiveType(value)) && !hasCoercible(shape(value)))) {
                return;
            }
            Signature declared = plannerContext.getMetadata().getDeclaredSignature(session, function);
            argumentResultShapes.put(NodeRef.of(node), variableShapes(declared, arguments));
            if (values.stream().allMatch(value -> CollationShapes.isTrivial(shape(value), getEffectiveType(value)))) {
                // only coercible strings, whose derivation the result keeps
                return;
            }
            String name = function.signature().getName().functionName();
            if (isBuiltinFunction(function) && COLLATION_BLIND_COMPARISONS.contains(name) && values.stream().anyMatch(value -> hasNonDefaultCollation(getEffectiveType(value)) || shape(value).firstNone().isPresent())) {
                throw semanticException(NOT_SUPPORTED, node, "%s does not support collated strings; cast them to varchar to compare code points", name);
            }
            Set<String> compared = ImmutableSet.<String>builder()
                    .addAll(declared.getTypeVariableConstraints().stream()
                            .filter(constraint -> constraint.isComparableRequired() || constraint.isOrderableRequired())
                            .map(TypeVariableConstraint::getName)
                            .iterator())
                    .addAll(comparedByOperators(function))
                    .build();
            if (compared.isEmpty()) {
                return;
            }
            ImmutableList.Builder<CollationShape> shapes = ImmutableList.builder();
            List<TypeTemplate> formals = declared.getArgumentTypes();
            for (int i = 0; i < arguments.size(); i++) {
                Expression argument = arguments.get(i);
                if (argument instanceof LambdaExpression) {
                    continue;
                }
                TypeTemplate formal = formals.get(Math.min(i, formals.size() - 1));
                collectVariableShapes(formal, shape(argument), compared, shapes);
            }
            try {
                CollationSemantics.determineShapes(shapes.build());
            }
            catch (CollationConflictException e) {
                throw collationMismatch(node, e);
            }
            rejectCollatedWithChar(node, values.stream().map(this::getEffectiveType).collect(toImmutableList()));
        }

        /// Records, for each lambda argument of a call, the shapes of the values its parameters
        /// receive, from the type variables the declared function type binds them to. A lambda
        /// over the elements of an array whose strings have derivation none therefore receives
        /// strings with derivation none, which fail where the lambda compares them.
        private void recordLambdaArgumentShapes(ResolvedFunction function, List<Expression> arguments)
        {
            if (arguments.stream().noneMatch(LambdaExpression.class::isInstance)) {
                return;
            }
            List<Expression> values = arguments.stream()
                    .filter(argument -> !(argument instanceof LambdaExpression))
                    .collect(toImmutableList());
            if (values.stream().allMatch(value -> CollationShapes.isTrivial(shape(value), getEffectiveType(value)) && !hasCoercible(shape(value)))) {
                return;
            }
            Signature declared = plannerContext.getMetadata().getDeclaredSignature(session, function);
            Map<String, CollationShape> variables = variableShapes(declared, arguments);
            List<TypeTemplate> formals = declared.getArgumentTypes();
            for (int i = 0; i < arguments.size() && !formals.isEmpty(); i++) {
                if (!(arguments.get(i) instanceof LambdaExpression lambda)) {
                    continue;
                }
                TypeTemplate formal = formals.get(Math.min(i, formals.size() - 1));
                if (!(formal instanceof TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters)) || !base.equals(FunctionType.NAME)) {
                    continue;
                }
                List<TypeTemplate> functionParameters = parameters.stream()
                        .filter(TemplateParameter.TypeArgument.class::isInstance)
                        .map(parameter -> ((TemplateParameter.TypeArgument) parameter).type())
                        .collect(toImmutableList());
                // the last parameter of a function type is its result
                List<CollationShape> inputShapes = functionParameters.subList(0, Math.max(0, functionParameters.size() - 1)).stream()
                        .map(parameter -> substitute(parameter, variables))
                        .collect(toImmutableList());
                lambdaArgumentShapes.put(NodeRef.of(lambda), inputShapes);
            }
        }

        private static boolean isBuiltinFunction(ResolvedFunction function)
        {
            return function.catalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE);
        }

        // the type variables a function compares through declared comparison operator dependencies,
        // such as the EQUAL on the keys that element_at uses to find a map entry
        private Set<String> comparedByOperators(ResolvedFunction function)
        {
            if (isTrinoSqlLanguageFunction(function.functionId())) {
                return ImmutableSet.of();
            }
            FunctionDependencyDeclaration dependencies = plannerContext.getMetadata().getFunctionDependencies(session, function.catalogHandle(), function.functionId(), function.signature());
            return dependencies.getOperatorDependencies().stream()
                    .filter(dependency -> COMPARISON_OPERATORS.contains(dependency.getOperatorType()))
                    .flatMap(dependency -> dependency.getArgumentTypes().stream())
                    .filter(TypeTemplate.TypeVariable.class::isInstance)
                    .map(argument -> ((TypeTemplate.TypeVariable) argument).name())
                    .collect(toImmutableSet());
        }

        // the combined shape of the arguments at each type variable of the declared signature
        private Map<String, CollationShape> variableShapes(Signature declared, List<Expression> arguments)
        {
            Set<String> variables = declared.getTypeVariableConstraints().stream()
                    .map(TypeVariableConstraint::getName)
                    .collect(toImmutableSet());
            Map<String, ImmutableList.Builder<CollationShape>> shapes = new LinkedHashMap<>();
            List<TypeTemplate> formals = declared.getArgumentTypes();
            for (int i = 0; i < arguments.size() && !formals.isEmpty(); i++) {
                Expression argument = arguments.get(i);
                if (argument instanceof LambdaExpression) {
                    continue;
                }
                TypeTemplate formal = formals.get(Math.min(i, formals.size() - 1));
                for (String variable : variables) {
                    ImmutableList.Builder<CollationShape> found = ImmutableList.builder();
                    collectVariableShapes(formal, shape(argument), ImmutableSet.of(variable), found);
                    shapes.computeIfAbsent(variable, _ -> ImmutableList.builder()).addAll(found.build());
                }
            }
            ImmutableMap.Builder<String, CollationShape> combined = ImmutableMap.builder();
            shapes.forEach((variable, found) -> {
                try {
                    combined.put(variable, CollationSemantics.combineShapes(found.build()));
                }
                catch (CollationConflictException e) {
                    // the conflict is reported where the values are compared
                    combined.put(variable, CollationShape.Opaque.OPAQUE);
                }
            });
            return combined.buildOrThrow();
        }

        // the shape of a declared result type, given the shapes of its type variables
        private static CollationShape substitute(TypeTemplate template, Map<String, CollationShape> variables)
        {
            return switch (template) {
                case TypeTemplate.TypeVariable(String name) -> variables.getOrDefault(name, CollationShape.Opaque.OPAQUE);
                case TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters) -> {
                    List<CollationShape> nested = parameters.stream()
                            .filter(TemplateParameter.TypeArgument.class::isInstance)
                            .map(parameter -> substitute(((TemplateParameter.TypeArgument) parameter).type(), variables))
                            .collect(toImmutableList());
                    if (nested.stream().allMatch(shape -> shape == CollationShape.Opaque.OPAQUE)) {
                        yield CollationShape.Opaque.OPAQUE;
                    }
                    yield switch (base) {
                        case StandardTypes.ARRAY -> nested.size() == 1 ? new ArrayShape(nested.getFirst()) : CollationShape.Opaque.OPAQUE;
                        case StandardTypes.MAP -> nested.size() == 2 ? new MapShape(nested.get(0), nested.get(1)) : CollationShape.Opaque.OPAQUE;
                        case StandardTypes.ROW -> new RowShape(nested);
                        default -> CollationShape.Opaque.OPAQUE;
                    };
                }
            };
        }

        /// Records the shape of a call's result when its declared result type is built from type
        /// variables bound by its arguments, so that the result carries the arguments' derivations.
        private void recordResultShape(Expression node, ResolvedFunction function, Type resultType)
        {
            Map<String, CollationShape> variables = argumentResultShapes.remove(NodeRef.of(node));
            if (variables == null || variables.isEmpty() || collationDerivations.containsKey(NodeRef.of(node))) {
                return;
            }
            Signature declared = plannerContext.getMetadata().getDeclaredSignature(session, function);
            CollationShape shape = substitute(declared.getReturnType(), variables);
            if (shape != CollationShape.Opaque.OPAQUE) {
                recordShape(node, CollationShapes.reconcile(shape, resultType), resultType);
            }
        }

        // the argument shapes found at the positions of the compared type variables in a formal type
        private static void collectVariableShapes(TypeTemplate formal, CollationShape shape, Set<String> variables, ImmutableList.Builder<CollationShape> shapes)
        {
            switch (formal) {
                case TypeTemplate.TypeVariable(String name) -> {
                    if (variables.contains(name)) {
                        shapes.add(shape);
                    }
                }
                case TypeTemplate.TypeApplication(String _, List<TemplateParameter> parameters) -> {
                    List<TypeTemplate> nested = parameters.stream()
                            .filter(TemplateParameter.TypeArgument.class::isInstance)
                            .map(parameter -> ((TemplateParameter.TypeArgument) parameter).type())
                            .collect(toImmutableList());
                    switch (shape) {
                        case ArrayShape(CollationShape element) when nested.size() == 1 -> collectVariableShapes(nested.getFirst(), element, variables, shapes);
                        case MapShape(CollationShape key, CollationShape value) when nested.size() == 2 -> {
                            collectVariableShapes(nested.get(0), key, variables, shapes);
                            collectVariableShapes(nested.get(1), value, variables, shapes);
                        }
                        case RowShape(List<CollationShape> fields) when nested.size() == fields.size() -> {
                            for (int i = 0; i < fields.size(); i++) {
                                collectVariableShapes(nested.get(i), fields.get(i), variables, shapes);
                            }
                        }
                        default -> {}
                    }
                }
            }
        }

        /// Records the shape of a row field selected from a row value.
        private void recordFieldShape(Expression node, Expression row, int fieldIndex, Type fieldType)
        {
            if (shape(row) instanceof RowShape(List<CollationShape> fields) && fieldIndex < fields.size()) {
                recordShape(node, fields.get(fieldIndex), fieldType);
            }
        }

        /// Applies collation determination between a map's keys and a string compared with them,
        /// and relabels the string to the determined collation.
        private void determineKeyCollation(Expression anchor, CollationShape keyShape, Expression key)
        {
            if (!(keyShape instanceof Leaf(Collated keys))) {
                return;
            }
            Collated value = collated(key).orElseThrow();
            Collation collation;
            try {
                collation = CollationSemantics.determine(ImmutableList.of(keys, value));
            }
            catch (CollationConflictException e) {
                throw collationMismatch(anchor, e);
            }
            relabel(ImmutableList.of(key), new Leaf(Collated.implicit(collation)));
        }

        /// Records that a node passes its operand's value through unchanged, so it has the operand's
        /// collation derivation.
        private void inheritCollation(Expression node, Expression operand)
        {
            CollationShape recorded = collationDerivations.get(NodeRef.of(operand));
            if (recorded != null) {
                collationDerivations.put(NodeRef.of(node), recorded);
            }
        }

        /// Requires a sort, grouping, or partitioning key to have a collation for each string it
        /// contains.
        private void requireCollation(Expression expression)
        {
            CollationShape shape = collationDerivations.get(NodeRef.of(expression));
            if (shape != null) {
                shape.firstNone().ifPresent(none -> {
                    throw collationMismatch(expression, new CollationConflictException(Derivation.IMPLICIT, none.conflicting()));
                });
            }
        }

        /// Combines the operands' collations, records the result's shape on the combining node,
        /// and returns the operand types after relabeling.
        private List<Type> combineCollationsInto(Expression node, List<Expression> operands)
        {
            CombinedCollation combined = combineCollations(node, operands);
            combined.shape().ifPresent(shape -> collationDerivations.put(NodeRef.of(node), shape));
            return combined.types();
        }

        private Type visitAtLocal(AtLocal node, Context context)
        {
            Type valueType = process(node.getValue(), context);
            Type resultType = switch (valueType) {
                case TimeType type -> createTimeWithTimeZoneType(type.getPrecision());
                case TimestampType type -> createTimestampWithTimeZoneType(type.getPrecision());
                case TimeWithTimeZoneType _, TimestampWithTimeZoneType _ -> valueType;
                default -> throw semanticException(TYPE_MISMATCH, node.getValue(), "Type of value must be a time or timestamp with or without time zone (actual %s)", valueType);
            };

            return setExpressionType(node, resultType);
        }

        private Type analyzeOverlaps(Expression value, OverlapsPredicate predicate, Expression anchor, Context context)
        {
            RowType leftRow = validatePeriod(value, process(value, context));
            RowType rightRow = validatePeriod(predicate.getRight(), process(predicate.getRight(), context));

            Type leftStart = leftRow.getFields().get(0).getType();
            Type leftEnd = leftRow.getFields().get(1).getType();
            Type rightStart = rightRow.getFields().get(0).getType();
            Type rightEnd = rightRow.getFields().get(1).getType();

            Optional<Type> commonStart = typeCoercion.getCommonSuperType(leftStart, rightStart);
            Optional<Type> commonEnd = typeCoercion.getCommonSuperType(leftEnd, rightEnd);
            if (commonStart.isEmpty() || commonEnd.isEmpty()) {
                throw semanticException(TYPE_MISMATCH, anchor, "Cannot apply OVERLAPS to %s and %s", leftRow, rightRow);
            }

            Type effectiveEnd;
            if (isInterval(commonEnd.get())) {
                try {
                    effectiveEnd = plannerContext.getMetadata().resolveOperator(charVarcharCoercion, ADD, ImmutableList.of(commonStart.get(), commonEnd.get())).signature().getReturnType();
                }
                catch (TrinoException e) {
                    throw semanticException(TYPE_MISMATCH, anchor, "Cannot apply OVERLAPS to %s and %s", leftRow, rightRow);
                }
            }
            else {
                effectiveEnd = commonEnd.get();
            }
            if (typeCoercion.getCommonSuperType(commonStart.get(), effectiveEnd).isEmpty()) {
                throw semanticException(TYPE_MISMATCH, anchor, "Cannot apply OVERLAPS to %s and %s", leftRow, rightRow);
            }

            RowType commonRow = RowType.anonymous(ImmutableList.of(commonStart.get(), commonEnd.get()));
            if (!leftRow.equals(commonRow)) {
                addOrReplaceExpressionCoercion(value, commonRow);
            }
            if (!rightRow.equals(commonRow)) {
                addOrReplaceExpressionCoercion(predicate.getRight(), commonRow);
            }

            return setExpressionType(anchor, BOOLEAN);
        }

        private RowType validatePeriod(Expression source, Type type)
        {
            if (!(type instanceof RowType row) || row.getFields().size() != 2) {
                throw semanticException(TYPE_MISMATCH, source, "OVERLAPS operand must be a row of two elements (actual %s)", type);
            }
            Type start = row.getFields().get(0).getType();
            Type end = row.getFields().get(1).getType();
            if (!isDatetime(start)) {
                throw semanticException(TYPE_MISMATCH, source, "OVERLAPS period start must be a datetime (actual %s)", start);
            }
            if (!isDatetime(end) && !isInterval(end)) {
                throw semanticException(TYPE_MISMATCH, source, "OVERLAPS period end must be a datetime or interval (actual %s)", end);
            }
            return row;
        }

        private static boolean isDatetime(Type type)
        {
            return type instanceof DateType
                    || type instanceof TimeType
                    || type instanceof TimeWithTimeZoneType
                    || type instanceof TimestampType
                    || type instanceof TimestampWithTimeZoneType;
        }

        private static boolean isInterval(Type type)
        {
            return type instanceof IntervalDayTimeType || type instanceof IntervalYearMonthType;
        }

        private Type visitTrim(Trim node, Context context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();

            argumentTypes.add(process(node.getTrimSource(), context));
            node.getTrimCharacter().ifPresent(trimChar -> argumentTypes.add(process(trimChar, context)));
            List<Type> actualTypes = argumentTypes.build();

            String functionName = node.getSpecification().getFunctionName();
            ResolvedFunction function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, functionName, actualTypes);
            Optional<Collated> resultCollation = stringResultCollation(node, function.signature().getReturnType(), ImmutableList.<Expression>builder()
                    .add(node.getTrimSource())
                    .addAll(node.getTrimCharacter().stream().iterator())
                    .build());

            List<Type> expectedTypes = function.signature().getArgumentTypes();
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

            return setStringResultType(node, function.signature().getReturnType(), resultCollation);
        }

        private Type visitOverlay(Overlay node, Context context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            argumentTypes.add(process(node.getValue(), context));
            argumentTypes.add(process(node.getReplacement(), context));
            argumentTypes.add(process(node.getStart(), context));
            node.getLength().ifPresent(length -> argumentTypes.add(process(length, context)));
            List<Type> actualTypes = argumentTypes.build();

            ResolvedFunction function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, OVERLAY_FUNCTION_NAME, actualTypes);
            Optional<Collated> resultCollation = stringResultCollation(node, function.signature().getReturnType(), ImmutableList.of(node.getValue(), node.getReplacement()));

            List<Type> expectedTypes = function.signature().getArgumentTypes();
            checkState(expectedTypes.size() == actualTypes.size(), "wrong argument number in the resolved signature");

            coerceType(node.getValue(), actualTypes.get(0), expectedTypes.get(0), "value argument of overlay function");
            coerceType(node.getReplacement(), actualTypes.get(1), expectedTypes.get(1), "replacement argument of overlay function");
            coerceType(node.getStart(), actualTypes.get(2), expectedTypes.get(2), "start argument of overlay function");
            if (node.getLength().isPresent()) {
                coerceType(node.getLength().get(), actualTypes.get(3), expectedTypes.get(3), "length argument of overlay function");
            }

            resolvedFunctions.put(NodeRef.of(node), function);

            return setStringResultType(node, function.signature().getReturnType(), resultCollation);
        }

        private Type visitFormat(Format node, Context context)
        {
            List<Type> arguments = node.getArguments().stream()
                    .map(expression -> process(expression, context))
                    .collect(toImmutableList());

            if (!(arguments.getFirst() instanceof VarcharType)) {
                throw semanticException(TYPE_MISMATCH, node.getArguments().getFirst(), "Type of first argument to format() must be VARCHAR (actual: %s)", arguments.getFirst());
            }

            for (int i = 1; i < arguments.size(); i++) {
                try {
                    plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, FORMAT_FUNCTION_NAME, ImmutableList.of(arguments.getFirst(), RowType.anonymous(arguments.subList(1, arguments.size()))));
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

        private Type visitParameter(Parameter node, Context context)
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
            // a parameter's value is a literal, so it has the literal's derivation
            inheritCollation(node, providedValue);
            return setExpressionType(node, resultType);
        }

        private Type visitExtract(Extract node, Context context)
        {
            Type type = process(node.getExpression(), context);
            Extract.Field field = node.getField();

            // An interval may only be extracted on a field its qualifier contains.
            if (type instanceof IntervalYearMonthType || type instanceof IntervalDayTimeType) {
                if (!intervalContainsField(type, field)) {
                    throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                }
                return setExpressionType(node, BIGINT);
            }

            switch (field) {
                case YEAR, MONTH -> {
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                }
                case DAY -> {
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                }
                case QUARTER, WEEK, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY, YEAR_OF_WEEK, YOW -> {
                    if (!(type instanceof DateType) &&
                            !(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                }
                case HOUR, MINUTE, SECOND -> {
                    if (!(type instanceof TimestampType) &&
                            !(type instanceof TimestampWithTimeZoneType) &&
                            !(type instanceof TimeType) &&
                            !(type instanceof TimeWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                }
                case TIMEZONE_MINUTE, TIMEZONE_HOUR -> {
                    if (!(type instanceof TimestampWithTimeZoneType) && !(type instanceof TimeWithTimeZoneType)) {
                        throw semanticException(TYPE_MISMATCH, node.getExpression(), "Cannot extract %s from %s", field, type);
                    }
                }
                default -> throw new UnsupportedOperationException("Unknown field: " + field);
            }

            return setExpressionType(node, BIGINT);
        }

        private static boolean intervalContainsField(Type type, Extract.Field field)
        {
            io.trino.spi.type.IntervalField extractField = switch (field) {
                case YEAR -> io.trino.spi.type.IntervalField.YEAR;
                case MONTH -> io.trino.spi.type.IntervalField.MONTH;
                case DAY -> io.trino.spi.type.IntervalField.DAY;
                case HOUR -> io.trino.spi.type.IntervalField.HOUR;
                case MINUTE -> io.trino.spi.type.IntervalField.MINUTE;
                case SECOND -> io.trino.spi.type.IntervalField.SECOND;
                default -> null;
            };
            if (extractField == null) {
                return false;
            }

            io.trino.spi.type.IntervalField start;
            io.trino.spi.type.IntervalField end;
            if (type instanceof IntervalYearMonthType yearMonth) {
                start = yearMonth.getStartField();
                end = yearMonth.getEndField();
            }
            else {
                IntervalDayTimeType dayTime = (IntervalDayTimeType) type;
                start = dayTime.getStartField();
                end = dayTime.getEndField();
            }
            return start.code() <= extractField.code() && extractField.code() <= end.code();
        }

        private static boolean isDateTimeType(Type type)
        {
            return isDatetime(type) || isInterval(type);
        }

        private Type visitTryExpression(TryExpression node, Context context)
        {
            // TRY is rewritten to lambda, and lambda is not supported in pattern recognition
            if (context.isPatternRecognition()) {
                throw semanticException(NOT_SUPPORTED, node, "TRY expression in pattern recognition context is not yet supported");
            }

            Type type = process(node.getInnerExpression(), context);
            inheritCollation(node, node.getInnerExpression());
            return setExpressionType(node, type);
        }

        private Type visitIntervalValueExpression(IntervalValueExpression node, Context context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);
            if ((!isDatetime(left) && !left.equals(UNKNOWN)) || (!isDatetime(right) && !right.equals(UNKNOWN)) ||
                    (left.equals(UNKNOWN) && right.equals(UNKNOWN))) {
                throw semanticException(TYPE_MISMATCH, node, "Qualified datetime difference requires datetime operands (actual: %s, %s)", left, right);
            }

            Type type;
            try {
                type = plannerContext.getTypeManager().getType(toTypeDescriptor(node.getType()));
            }
            catch (TypeNotFoundException e) {
                throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", node.getType());
            }
            if (type instanceof IntervalYearMonthType) {
                throw semanticException(NOT_SUPPORTED, node, "Computing a year-month interval difference of datetimes is not yet supported");
            }

            // DATE has no subtraction operator. Measure its elapsed difference from midnight timestamps.
            Type leftArgument = left instanceof DateType ? createTimestampType(0) : left;
            Type rightArgument = right instanceof DateType ? createTimestampType(0) : right;
            BoundSignature signature;
            try {
                signature = plannerContext.getMetadata().resolveOperator(charVarcharCoercion, OperatorType.SUBTRACT, ImmutableList.of(leftArgument, rightArgument)).signature();
                plannerContext.getMetadata().getCoercion(charVarcharCoercion, signature.getReturnType(), type);
            }
            catch (OperatorNotFoundException e) {
                throw semanticException(TYPE_MISMATCH, node, e, "%s", e.getMessage());
            }
            coerceType(node.getLeft(), left, signature.getArgumentTypes().get(0), "Left datetime operand");
            coerceType(node.getRight(), right, signature.getArgumentTypes().get(1), "Right datetime operand");
            return setExpressionType(node, type);
        }

        private Type visitCast(Cast node, Context context)
        {
            Type type;
            try {
                type = plannerContext.getTypeManager().getType(toTypeDescriptor(node.getType()));
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
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, value, type);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            return setExpressionType(node, type);
        }

        private Type analyzeIn(Expression value, InPredicate predicate, Expression anchor, Context context)
        {
            Expression valueList = predicate.getValueList();

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
                        anchor,
                        new ScalarInputDescriptor(Optional.empty(), context.getPatternRecognitionContext().navigation())));
            }

            if (valueList instanceof InListExpression inListExpression) {
                List<Expression> operands = ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build();
                // IN compares its operands, so their collations must determine one collation
                for (Expression operand : operands) {
                    process(operand, context);
                }
                determineCollation(anchor, operands);
                Type type = coerceToSingleType(
                        context,
                        anchor,
                        "IN value and list items",
                        operands);
                setExpressionType(inListExpression, type);
            }
            else if (valueList instanceof SubqueryExpression subqueryExpression) {
                subqueryInPredicates.add(new OperandAndPredicate(value, predicate));
                analyzePredicateWithSubquery(predicate, process(value, context), subqueryExpression, context);
            }
            else {
                throw new IllegalArgumentException("Unexpected value list type for InPredicate: " + valueList.getClass().getName());
            }

            return setExpressionType(anchor, BOOLEAN);
        }

        private Type visitSubqueryExpression(SubqueryExpression node, Context context)
        {
            Type type = analyzeSubquery(node, context);

            // the implied type of a scalar subquery is that of the unique field in the single-column row
            if (type instanceof RowType rowType && rowType.getFields().size() == 1) {
                type = rowType.getFieldTypes().getFirst();
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
        private Type analyzePredicateWithSubquery(Predicate predicate, Type declaredValueType, SubqueryExpression subquery, Context context)
        {
            Type valueRowType = declaredValueType;
            if (!(declaredValueType instanceof RowType) && !(declaredValueType instanceof UnknownType)) {
                valueRowType = RowType.anonymous(ImmutableList.of(declaredValueType));
            }

            Type subqueryType = analyzeSubquery(subquery, context);
            setExpressionType(subquery, subqueryType);

            Optional<Type> commonType = typeCoercion.getCommonSuperType(valueRowType, subqueryType);

            if (commonType.isEmpty()) {
                throw semanticException(TYPE_MISMATCH, predicate, "Value expression and result of subquery must be of the same type: %s vs %s", valueRowType, subqueryType);
            }

            Optional<Type> valueCoercion = Optional.empty();
            if (!valueRowType.equals(commonType.get())) {
                valueCoercion = commonType;
            }

            Optional<Type> subQueryCoercion = Optional.empty();
            if (!subqueryType.equals(commonType.get())) {
                subQueryCoercion = commonType;
            }

            predicateCoercions.put(NodeRef.of(predicate), new PredicateCoercions(valueRowType, valueCoercion, subQueryCoercion));

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

        private Type visitExists(ExistsPredicate node, Context context)
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

        private Type visitUniquePredicate(UniquePredicate node, Context context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node, context.getCorrelationSupport());
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getScope())
                    .build();

            List<RowType.Field> fields = analyzer.analyze(node.getSubquery(), subqueryScope)
                    .getRelationType()
                    .getAllFields().stream()
                    .map(field -> field.getName()
                            .map(name -> RowType.field(name, field.getType()))
                            .orElseGet(() -> RowType.field(field.getType())))
                    .collect(toImmutableList());

            for (RowType.Field field : fields) {
                if (!field.getType().isComparable()) {
                    throw semanticException(TYPE_MISMATCH, node, "Type [%s] must be comparable in order to be used in UNIQUE predicate", field.getType());
                }
            }

            setExpressionType(node.getSubquery(), RowType.from(fields));

            uniquePredicates.add(NodeRef.of(node));

            return setExpressionType(node, BOOLEAN);
        }

        private Type analyzeQuantifiedComparison(Expression value, QuantifiedComparisonPredicate predicate, Expression anchor, Context context)
        {
            quantifiedComparisons.add(new OperandAndPredicate(value, predicate));

            Type declaredValueType = process(value, context);
            Type comparisonType = analyzePredicateWithSubquery(predicate, declaredValueType, (SubqueryExpression) predicate.getSubquery(), context);

            switch (predicate.getOperator()) {
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                    if (!comparisonType.isOrderable()) {
                        throw semanticException(TYPE_MISMATCH, anchor, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                }
                case EQUAL, NOT_EQUAL -> {
                    if (!comparisonType.isComparable()) {
                        throw semanticException(TYPE_MISMATCH, anchor, "Type [%s] must be comparable in order to be used in quantified comparison", comparisonType);
                    }
                }
            }

            return setExpressionType(anchor, BOOLEAN);
        }

        private Type analyzeMatchPredicate(Expression value, MatchPredicate predicate, Expression anchor, Context context)
        {
            matchPredicates.add(new OperandAndPredicate(value, predicate));

            Type declaredValueType = process(value, context);
            Type comparisonType = analyzePredicateWithSubquery(predicate, declaredValueType, (SubqueryExpression) predicate.getSubquery(), context);

            if (!comparisonType.isComparable()) {
                throw semanticException(TYPE_MISMATCH, anchor, "Type [%s] must be comparable in order to be used in MATCH predicate", comparisonType);
            }

            return setExpressionType(anchor, BOOLEAN);
        }

        private Type visitFieldReference(FieldReference node, Context context)
        {
            ResolvedField field = baseScope.getField(node.getFieldIndex());
            return handleResolvedField(node, field, context);
        }

        private Type visitLambdaExpression(LambdaExpression node, Context context)
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
                throw semanticException(
                        INVALID_PARAMETER_USAGE,
                        node,
                        "Expected a lambda that takes %s argument(s) but got %s",
                        types.size(),
                        lambdaArguments.size());
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < lambdaArguments.size(); i++) {
                LambdaArgumentDeclaration lambdaArgument = lambdaArguments.get(i);
                Type type = types.get(i);
                fields.add(Field.newUnqualified(lambdaArgument.getName().getValue(), type));
                setExpressionType(lambdaArgument, type);
                List<CollationShape> argumentShapes = lambdaArgumentShapes.getOrDefault(NodeRef.of(node), ImmutableList.of());
                if (i < argumentShapes.size()) {
                    CollationShape argumentShape = CollationShapes.reconcile(argumentShapes.get(i), type);
                    if (!argumentShape.equals(CollationShapes.implicit(type))) {
                        lambdaParameterShapes.put(NodeRef.of(lambdaArgument), argumentShape);
                    }
                }
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
                ResolvedField resolvedField = lambdaScope.resolveField(lambdaArgument, QualifiedName.of(lambdaArgument.getName().getValue()), ImmutableSet::of);
                fieldToLambdaArgumentDeclaration.put(FieldId.from(resolvedField), lambdaArgument);
            }

            Type returnType = process(node.getBody(), context.inLambda(lambdaScope, fieldToLambdaArgumentDeclaration.buildOrThrow()));
            FunctionType functionType = new FunctionType(types, returnType);
            return setExpressionType(node, functionType);
        }

        private Type visitExpression(Expression node)
        {
            throw semanticException(NOT_SUPPORTED, node, "not yet implemented: %s", node.getClass().getName());
        }

        private Type visitGroupingOperation(GroupingOperation node, Context context)
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

        private Type visitJsonExists(JsonExists node, Context context)
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
                function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_EXISTS_FUNCTION_NAME, argumentTypes);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);
            Type type = function.signature().getReturnType();

            return setExpressionType(node, type);
        }

        private Type visitJsonValue(JsonValue node, Context context)
        {
            List<Type> pathInvocationArgumentTypes = analyzeJsonPathInvocation("JSON_VALUE", node, node.getJsonPathInvocation(), context);
            Type returnedType = analyzeJsonValueExpression(
                    "JSON_VALUE",
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
                String callerName,
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
                    returnedType = plannerContext.getTypeManager().getType(toTypeDescriptor(declaredReturnedType.get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", declaredReturnedType.get());
                }
            }

            if ((!isCharacterStringType(returnedType) &&
                    !isNumericType(returnedType) &&
                    !returnedType.equals(BOOLEAN) &&
                    !isDateTimeType(returnedType)) ||
                    returnedType instanceof IntervalDayTimeType ||
                    returnedType instanceof IntervalYearMonthType) {
                throw semanticException(TYPE_MISMATCH, node, "Invalid return type of function JSON_VALUE: %s", declaredReturnedType.get());
            }

            Type resultType = pathAnalysis.getType(pathAnalysis.getPath());
            if (resultType != null && !resultType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, resultType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Return type of JSON path: %s incompatible with return type of function JSON_VALUE: %s", resultType, returnedType);
                }
            }

            // validate default values for empty and error behavior
            Type emptyDefaultType = returnedType;
            if (declaredEmptyDefault.isPresent()) {
                Expression emptyDefault = declaredEmptyDefault.get();
                if (emptyBehavior != DEFAULT) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, emptyDefault, "Default value specified for %s ON EMPTY behavior", emptyBehavior);
                }
                verifyNoSubqueriesInDefault(emptyDefault, callerName);
                emptyDefaultType = process(emptyDefault, context);
                verifyDefaultCoercion(emptyDefault, emptyDefaultType, returnedType, "Function %s default ON EMPTY result".formatted(callerName));
            }

            Type errorDefaultType = returnedType;
            if (declaredErrorDefault.isPresent()) {
                Expression errorDefault = declaredErrorDefault.get();
                if (errorBehavior.isEmpty()) {
                    throw new IllegalStateException("error default specified without error behavior specified");
                }
                if (errorBehavior.orElseThrow() != DEFAULT) {
                    throw semanticException(INVALID_FUNCTION_ARGUMENT, errorDefault, "Default value specified for %s ON ERROR behavior", errorBehavior.orElseThrow());
                }
                verifyNoSubqueriesInDefault(errorDefault, callerName);
                errorDefaultType = process(errorDefault, context);
                verifyDefaultCoercion(errorDefault, errorDefaultType, returnedType, "Function %s default ON ERROR result".formatted(callerName));
            }

            // pass remaining information in the node : empty behavior, empty default, error behavior, error default
            List<Type> argumentTypes = ImmutableList.<Type>builder()
                    .addAll(pathInvocationArgumentTypes)
                    .add(returnedType) // return type anchor
                    .add(TINYINT) // empty behavior: enum encoded as integer value
                    .add(new FunctionType(ImmutableList.of(), emptyDefaultType)) // empty default
                    .add(TINYINT) // error behavior: enum encoded as integer value
                    .add(new FunctionType(ImmutableList.of(), errorDefaultType)) // error default
                    .build();

            // resolve function
            ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_VALUE_FUNCTION_NAME, argumentTypes);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            return function.signature().getReturnType();
        }

        private void verifyDefaultCoercion(Expression defaultValue, Type defaultType, Type returnedType, String message)
        {
            if (defaultType.equals(returnedType)) {
                return;
            }
            try {
                plannerContext.getMetadata().getCoercion(charVarcharCoercion, defaultType, returnedType);
            }
            catch (OperatorNotFoundException e) {
                throw semanticException(TYPE_MISMATCH, defaultValue, "%s must evaluate to a %s (actual: %s)", message, returnedType, defaultType);
            }
        }

        private static void verifyNoSubqueriesInDefault(Expression expression, String callerName)
        {
            List<SubqueryExpression> subqueries = extractExpressions(ImmutableList.of(expression), SubqueryExpression.class);
            if (!subqueries.isEmpty()) {
                throw semanticException(UNSUPPORTED_SUBQUERY, subqueries.getFirst(), "Subqueries are not supported in %s default expressions", callerName);
            }
        }

        private Type visitJsonConstructor(JsonConstructor node, Context context)
        {
            Expression inputExpression = node.getExpression();
            Type inputType = process(inputExpression, context);

            ResolvedFunction inputFunction = getInputFunction(inputType, node.getFormat(), inputExpression);
            Type expectedType = inputFunction.signature().getArgumentType(0);
            coerceType(inputExpression, inputType, expectedType, "JSON input argument");
            jsonInputFunctions.put(NodeRef.of(inputExpression), inputFunction);

            ResolvedFunction outputFunction = getOutputFunction(JSON, JsonFormat.JSON, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            return setExpressionType(node, JSON);
        }

        private Type visitJsonQuery(JsonQuery node, Context context)
        {
            List<Type> pathInvocationArgumentTypes = analyzeJsonPathInvocation("JSON_QUERY", node, node.getJsonPathInvocation(), context);
            Type returnedType = analyzeJsonQueryExpression(
                    node,
                    node.getWrapperBehavior(),
                    node.getQuotesBehavior(),
                    pathInvocationArgumentTypes,
                    node.getReturnedType(),
                    node.getOutputFormat(),
                    JSON.equals(getExpressionType(node.getJsonPathInvocation().getInputExpression())));
            return setExpressionType(node, returnedType);
        }

        private Type visitJsonSerialize(JsonSerialize node, Context context)
        {
            Expression inputExpression = node.getExpression();
            Type inputType = process(inputExpression, context);

            ResolvedFunction inputFunction = getInputFunction(inputType, node.getInputFormat(), inputExpression);
            Type expectedType = inputFunction.signature().getArgumentType(0);
            coerceType(inputExpression, inputType, expectedType, "JSON_SERIALIZE input argument");
            jsonInputFunctions.put(NodeRef.of(inputExpression), inputFunction);

            Type returnedType = VARCHAR;
            if (node.getReturnedType().isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeDescriptor(node.getReturnedType().get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", node.getReturnedType().get());
                }
            }

            // isStringType is the union of character strings (CHAR/VARCHAR) and VARBINARY;
            // JSON is neither. SQL:2023 §6.37 requires the returned type to be a string type.
            if (!isStringType(returnedType)) {
                throw semanticException(TYPE_MISMATCH, node, "Invalid return type of function JSON_SERIALIZE: %s", returnedType);
            }

            JsonFormat outputFormat = node.getOutputFormat().orElse(JsonFormat.JSON);
            ResolvedFunction outputFunction = getOutputFunction(returnedType, outputFormat, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            Type outputType = outputFunction.signature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot return type %s from JSON_SERIALIZE function", returnedType);
                }
            }

            return setExpressionType(node, returnedType);
        }

        private Type analyzeJsonQueryExpression(
                Node node,
                JsonQuery.ArrayWrapperBehavior wrapperBehavior,
                Optional<JsonQuery.QuotesBehavior> quotesBehavior,
                List<Type> pathInvocationArgumentTypes,
                Optional<DataType> declaredReturnedType,
                Optional<JsonFormat> declaredOutputFormat,
                boolean jsonTypedInput)
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
                function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_QUERY_FUNCTION_NAME, argumentTypes);
            }
            catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    throw e;
                }
                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            // analyze returned type and format
            Type returnedType;
            if (declaredReturnedType.isPresent()) {
                try {
                    returnedType = plannerContext.getTypeManager().getType(toTypeDescriptor(declaredReturnedType.get()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", declaredReturnedType.get());
                }
            }
            else if (jsonTypedInput) {
                // SQL:2023 §6.35 SR 1: when the input is JSON-typed and no RETURNING clause is given,
                // the implicit returned type is JSON
                returnedType = JSON;
            }
            else {
                returnedType = VARCHAR;
            }

            // SQL:2023 §6.35 SR 3: if the effective returned type is JSON, the quotes behavior shall be KEEP.
            // OMIT QUOTES would require emitting a bare unquoted scalar, which is not valid JSON; the spec
            // closes the hole at analysis time, not at runtime (§9.44, which handles the JSON target, has no
            // QUOTES parameter).
            if (quotesBehavior.filter(behavior -> behavior == JsonQuery.QuotesBehavior.OMIT).isPresent() && JSON.equals(returnedType)) {
                throw semanticException(INVALID_FUNCTION_ARGUMENT, node, "OMIT QUOTES behavior is not allowed when JSON_QUERY returns JSON");
            }

            JsonFormat outputFormat = declaredOutputFormat.orElse(JsonFormat.JSON); // default

            // resolve function to format output
            ResolvedFunction outputFunction = getOutputFunction(returnedType, outputFormat, node);
            jsonOutputFunctions.put(NodeRef.of(node), outputFunction);

            // cast the output value to the declared returned type if necessary
            Type outputType = outputFunction.signature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, outputType, returnedType);
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
            Type expectedType = inputFunction.signature().getArgumentType(0);
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
                // - parameters with explicit or implicit FORMAT, are converted to JSON (type JSON)
                // - all other parameters are cast to VARCHAR
                Type passedType;

                if (!uniqueNames.add(parameterName)) {
                    throw semanticException(DUPLICATE_PARAMETER_NAME, pathParameter.getName(), "%s JSON path parameter is specified more than once", parameterName);
                }

                if (parameter instanceof LambdaExpression) {
                    throw semanticException(NOT_SUPPORTED, parameter, "%s is not supported as JSON path parameter", parameter.getClass().getSimpleName());
                }
                Type parameterType = process(parameter, context);
                // JSON-typed values and SQL/JSON-returning functions are consumed as JSON by default.
                if (parameterFormat.isEmpty() && (parameterType.equals(JSON) || parameter instanceof JsonQuery ||
                        parameter instanceof JsonObject ||
                        parameter instanceof JsonArray)) {
                    parameterFormat = Optional.of(JsonFormat.JSON);
                }
                if (parameterFormat.isPresent()) {
                    // resolve function to read the parameter as JSON
                    ResolvedFunction parameterInputFunction = getInputFunction(parameterType, parameterFormat.get(), parameter);
                    Type expectedParameterType = parameterInputFunction.signature().getArgumentType(0);
                    coerceType(parameter, parameterType, expectedParameterType, format("%s function JSON path parameter", functionName));
                    jsonInputFunctions.put(NodeRef.of(parameter), parameterInputFunction);
                    passedType = JSON;
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
                    else if (isDatetime(parameterType)) {
                        passedType = parameterType;
                    }
                    else {
                        try {
                            plannerContext.getMetadata().getCoercion(charVarcharCoercion, parameterType, VARCHAR);
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
                    session,
                    createConstantAnalyzer(plannerContext, accessControl, session, ExpressionAnalyzer.this.parameters, WarningCollector.NOOP))
                    .analyzeJsonPath(jsonPathInvocation.getJsonPath(), typesMap);
            jsonPathAnalyses.put(NodeRef.of(node), pathAnalysis);

            return ImmutableList.of(
                    JSON, // input expression
                    plannerContext.getTypeManager().getType(new TypeDescriptor(SqlJsonPathType.NAME)), // parsed JSON path representation
                    parametersRowType); // passed parameters
        }

        private ResolvedFunction getInputFunction(Type type, JsonFormat format, Node node)
        {
            String name = switch (format) {
                case JSON -> {
                    if (type.equals(JSON)) {
                        yield JSON_TO_JSON;
                    }
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
                return plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, name, ImmutableList.of(type, BOOLEAN));
            }
            catch (TrinoException e) {
                throw new TrinoException(TYPE_MISMATCH, extractLocation(node), format("Cannot read input of type %s as JSON using formatting %s", type, format), e);
            }
        }

        private ResolvedFunction getOutputFunction(Type type, JsonFormat format, Node node)
        {
            String name = switch (format) {
                case JSON -> {
                    if (type.equals(JSON)) {
                        yield JSON_TO_JSON_OUTPUT;
                    }
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
                return plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, name, ImmutableList.of(JSON, TINYINT, BOOLEAN));
            }
            catch (TrinoException e) {
                throw new TrinoException(TYPE_MISMATCH, extractLocation(node), format("Cannot output JSON value as %s using formatting %s", type, format), e);
            }
        }

        private Type visitJsonObject(JsonObject node, Context context)
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

                if (value instanceof LambdaExpression) {
                    throw semanticException(NOT_SUPPORTED, value, "%s is not supported as JSON object value", value.getClass().getSimpleName());
                }

                // types accepted for values of a JSON object:
                // - values of types numeric, string, and boolean are passed as-is
                // - values with explicit or implicit FORMAT, are converted to JSON (type JSON)
                // - all other values are cast to VARCHAR

                Type valueType = process(value, context);
                // JSON-typed values and SQL/JSON-returning functions are consumed as JSON by default.
                if (format.isEmpty() && (valueType.equals(JSON) || value instanceof JsonQuery ||
                        value instanceof JsonObject ||
                        value instanceof JsonArray)) {
                    format = Optional.of(JsonFormat.JSON);
                }

                if (format.isPresent()) {
                    // resolve function to read the value as JSON
                    ResolvedFunction inputFunction = getInputFunction(valueType, format.get(), value);
                    Type expectedValueType = inputFunction.signature().getArgumentType(0);
                    coerceType(value, valueType, expectedValueType, "value passed to JSON_OBJECT function");
                    jsonInputFunctions.put(NodeRef.of(value), inputFunction);
                    valueType = JSON;
                }
                else {
                    if (isStringType(valueType)) {
                        if (!isCharacterStringType(valueType)) {
                            throw semanticException(NOT_SUPPORTED, value, "Unsupported type of value passed to JSON_OBJECT function: %s", valueType.getDisplayName());
                        }
                    }

                    if (!isStringType(valueType) && !isNumericType(valueType) && !valueType.equals(BOOLEAN)) {
                        try {
                            plannerContext.getMetadata().getCoercion(charVarcharCoercion, valueType, VARCHAR);
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
                function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_OBJECT_FUNCTION_NAME, argumentTypes);
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
                    returnedType = plannerContext.getTypeManager().getType(toTypeDescriptor(node.getReturnedType().get()));
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
            Type outputType = outputFunction.signature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot return type %s from JSON_OBJECT function", returnedType);
                }
            }

            return setExpressionType(node, returnedType);
        }

        private Type visitJsonArray(JsonArray node, Context context)
        {
            // TODO verify parameter count? Is there a limit on Row size?

            ImmutableList.Builder<RowType.Field> elementFields = ImmutableList.builder();

            for (JsonArrayElement arrayElement : node.getElements()) {
                Expression element = arrayElement.getValue();
                Optional<JsonFormat> format = arrayElement.getFormat();

                if (element instanceof LambdaExpression) {
                    throw semanticException(NOT_SUPPORTED, element, "%s is not supported as JSON array element", element.getClass().getSimpleName());
                }

                // types accepted for elements of a JSON array:
                // - values of types numeric, string, and boolean are passed as-is
                // - values with explicit or implicit FORMAT, are converted to JSON (type JSON)
                // - all other values are cast to VARCHAR

                Type elementType = process(element, context);
                // JSON-typed values and SQL/JSON-returning functions are consumed as JSON by default.
                if (format.isEmpty() && (elementType.equals(JSON) || element instanceof JsonQuery ||
                        element instanceof JsonObject ||
                        element instanceof JsonArray)) {
                    format = Optional.of(JsonFormat.JSON);
                }

                if (format.isPresent()) {
                    // resolve function to read the value as JSON
                    ResolvedFunction inputFunction = getInputFunction(elementType, format.get(), element);
                    Type expectedElementType = inputFunction.signature().getArgumentType(0);
                    coerceType(element, elementType, expectedElementType, "value passed to JSON_ARRAY function");
                    jsonInputFunctions.put(NodeRef.of(element), inputFunction);
                    elementType = JSON;
                }
                else {
                    if (isStringType(elementType)) {
                        if (!isCharacterStringType(elementType)) {
                            throw semanticException(NOT_SUPPORTED, element, "Unsupported type of value passed to JSON_ARRAY function: %s", elementType.getDisplayName());
                        }
                    }

                    if (!isStringType(elementType) && !isNumericType(elementType) && !elementType.equals(BOOLEAN)) {
                        try {
                            plannerContext.getMetadata().getCoercion(charVarcharCoercion, elementType, VARCHAR);
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
                function = plannerContext.getMetadata().resolveBuiltinFunction(charVarcharCoercion, JSON_ARRAY_FUNCTION_NAME, argumentTypes);
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
                    returnedType = plannerContext.getTypeManager().getType(toTypeDescriptor(node.getReturnedType().get()));
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
            Type outputType = outputFunction.signature().getReturnType();
            if (!outputType.equals(returnedType)) {
                try {
                    plannerContext.getMetadata().getCoercion(charVarcharCoercion, outputType, returnedType);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, node, "Cannot return type %s from JSON_ARRAY function", returnedType);
                }
            }

            return setExpressionType(node, returnedType);
        }

        private Type getOperator(Context context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypesBuilder = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypesBuilder.add(process(expression, context));
            }
            List<Type> argumentTypes = argumentTypesBuilder.build();
            if (isComparison(operatorType)) {
                argumentTypes = determineCollation(node, ImmutableList.copyOf(arguments));
            }
            else {
                // operands relabeled before resolution, such as a map subscript's index, are
                // resolved with their relabeled types
                argumentTypes = Arrays.stream(arguments).map(this::getEffectiveType).collect(toImmutableList());
            }

            BoundSignature operatorSignature;
            try {
                operatorSignature = plannerContext.getMetadata().resolveOperator(charVarcharCoercion, operatorType, argumentTypes).signature();
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

        private static boolean isComparison(OperatorType operatorType)
        {
            return switch (operatorType) {
                case EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, IDENTICAL, COMPARISON_UNORDERED_FIRST, COMPARISON_UNORDERED_LAST -> true;
                default -> false;
            };
        }

        private void coerceType(Expression expression, Type actualType, Type expectedType, String message)
        {
            // an operand relabeled to a collation is used with that collation
            Type effectiveType = expressionCoercions.getOrDefault(NodeRef.of(expression), actualType);
            if (effectiveType.equals(expectedType)) {
                return;
            }
            // A string function declared over the default collation receives a collated argument's
            // bytes as default-collation strings, including the strings nested in arrays, maps, and
            // rows. The caller derives the result's collation from the argument before this coercion
            if (hasNonDefaultCollation(effectiveType)
                    && !hasNonDefaultCollation(expectedType)
                    && typeCoercion.canCoerce(withoutCollations(effectiveType, plannerContext.getTypeManager()::getType), expectedType)) {
                addOrReplaceExpressionCoercion(expression, expectedType);
                return;
            }
            if (!typeCoercion.canCoerce(effectiveType, expectedType)) {
                throw semanticException(TYPE_MISMATCH, expression, "%s must evaluate to a %s (actual: %s)", message, expectedType, effectiveType);
            }
            addOrReplaceExpressionCoercion(expression, expectedType);
        }

        private void coerceType(Context context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private Type coerceToSingleType(Context context, Expression node, String message, Expression first, Expression second)
        {
            Type firstType = UNKNOWN;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = UNKNOWN;
            if (second != null) {
                secondType = process(second, context);
            }
            if (first != null && second != null) {
                List<Type> aligned = combineCollationsInto(node, ImmutableList.of(first, second));
                firstType = aligned.get(0);
                secondType = aligned.get(1);
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

        private Type coerceToSingleType(Context context, Expression node, String description, List<Expression> expressions)
        {
            // determine super type
            Type superType = UNKNOWN;

            for (Expression expression : expressions) {
                process(expression, context);
            }
            List<Type> aligned = combineCollationsInto(node, expressions);

            // Use LinkedHashMultimap to preserve order in which expressions are analyzed within IN list
            Multimap<Type, NodeRef<Expression>> typeExpressions = LinkedHashMultimap.create();
            for (int i = 0; i < expressions.size(); i++) {
                // We need to wrap as NodeRef since LinkedHashMultimap does not allow duplicated values
                typeExpressions.put(aligned.get(i), NodeRef.of(expressions.get(i)));
            }

            Set<Type> types = typeExpressions.keySet();

            for (Type type : types) {
                Optional<Type> newSuperType = typeCoercion.getCommonSuperType(superType, type);
                if (newSuperType.isEmpty()) {
                    throw semanticException(
                            TYPE_MISMATCH,
                            Iterables.get(typeExpressions.get(type), 0).getNode(),
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
                        throw semanticException(
                                TYPE_MISMATCH,
                                Iterables.get(coercionCandidates, 0).getNode(),
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

    private static void validateIntervalQualifier(IntervalQualifier qualifier)
    {
        if (qualifier instanceof CompositeIntervalQualifier composite) {
            boolean valid = (composite.getFrom() instanceof IntervalField.Year() && composite.getTo() instanceof IntervalField.Month()) ||
                    (composite.getFrom() instanceof IntervalField.Day() && (
                            composite.getTo() instanceof IntervalField.Hour() ||
                                    composite.getTo() instanceof IntervalField.Minute() ||
                                    composite.getTo() instanceof IntervalField.Second)) ||
                    (composite.getFrom() instanceof IntervalField.Hour() && (
                            composite.getTo() instanceof IntervalField.Minute() ||
                                    composite.getTo() instanceof IntervalField.Second)) ||
                    (composite.getFrom() instanceof IntervalField.Minute() && composite.getTo() instanceof IntervalField.Second);

            if (!valid) {
                throw semanticException(SYNTAX_ERROR, qualifier, "Invalid INTERVAL qualifier");
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        analyzer.analyze(expression, scope, labels, false);

        updateAnalysis(analysis, analyzer, session, accessControl);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getMatchPredicates(),
                analyzer.getUniquePredicates(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpressions(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Iterable<Expression> expressions,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            QueryType queryType)
    {
        Analysis analysis = new Analysis(null, parameters, queryType);
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        for (Expression expression : expressions) {
            analyzer.analyze(
                    expression,
                    Scope.builder()
                            .withRelationType(RelationId.anonymous(), new RelationType())
                            .build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getMatchPredicates(),
                analyzer.getUniquePredicates(),
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        analyzer.analyze(expression, scope, correlationSupport);

        updateAnalysis(analysis, analyzer, session, accessControl);
        analysis.addExpressionFields(expression, analyzer.getSourceFields());

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getMatchPredicates(),
                analyzer.getUniquePredicates(),
                analyzer.getWindowFunctions());
    }

    public static boolean analyzeJsonWildcardAccessor(
            Session session,
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            AccessControl accessControl,
            Scope scope,
            Analysis analysis,
            Expression target,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        boolean recognized = analyzer.analyzeJsonWildcardTarget(target, scope, correlationSupport);
        if (recognized) {
            updateAnalysis(analysis, analyzer, session, accessControl);
        }
        return recognized;
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        RowType parametersRowType = analyzer.analyzeJsonPathInvocation(node, scope, correlationSupport);
        updateAnalysis(analysis, analyzer, session, accessControl);
        return new ParametersTypeAndAnalysis(
                parametersRowType,
                new ExpressionAnalysis(
                        analyzer.getExpressionTypes(),
                        analyzer.getSubqueryInPredicates(),
                        analyzer.getSubqueries(),
                        analyzer.getExistsSubqueries(),
                        analyzer.getQuantifiedComparisons(),
                        analyzer.getMatchPredicates(),
                        analyzer.getUniquePredicates(),
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        Type type = analyzer.analyzeJsonValueExpression(column, pathAnalysis, scope, correlationSupport);
        updateAnalysis(analysis, analyzer, session, accessControl);
        return new TypeAndAnalysis(type, new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getMatchPredicates(),
                analyzer.getUniquePredicates(),
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
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
                (node, _) -> {
                    throw semanticException(errorCode, node, "%s", message);
                },
                session,
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
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plannerContext, accessControl, statementAnalyzerFactory, analysis, session, warningCollector);
        analyzer.analyzeWindow(window, scope, originalNode, correlationSupport);

        updateAnalysis(analysis, analyzer, session, accessControl);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getMatchPredicates(),
                analyzer.getUniquePredicates(),
                analyzer.getWindowFunctions());
    }

    private static void updateAnalysis(Analysis analysis, ExpressionAnalyzer analyzer, Session session, AccessControl accessControl)
    {
        analysis.addTypes(analyzer.getExpressionTypes());
        analysis.addCoercions(
                analyzer.getExpressionCoercions(),
                analyzer.getSortKeyCoercionsForFrameBoundCalculation(),
                analyzer.getSortKeyCoercionsForFrameBoundComparison());
        analysis.addNullIfComparisonTypes(analyzer.getNullIfComparisonTypes());
        analysis.addUncollatedExpressions(analyzer.getUncollatedExpressions());
        analysis.addFrameBoundCalculations(analyzer.getFrameBoundCalculations());
        analyzer.getResolvedFunctions().forEach((key, value) -> analysis.addResolvedFunction(key.getNode(), value, session.getUser()));
        analyzer.getMethodCallReceivers().forEach((key, value) -> analysis.addMethodCallReceiver(key.getNode(), value));
        analyzer.getArgumentBindings().forEach((key, value) -> analysis.setArgumentBinding(key.getNode(), value));
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
        analyzer.getJsonSimplifiedAccessors().forEach((key, value) -> analysis.setJsonSimplifiedAccessor(key.getNode(), value));
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
                parameters,
                node -> semanticException(errorCode, node, "%s", message),
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            PlannerContext plannerContext,
            AccessControl accessControl,
            Session session,
            Map<NodeRef<Parameter>, Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return new ExpressionAnalyzer(
                plannerContext,
                accessControl,
                (node, _) -> {
                    throw statementAnalyzerRejection.apply(node);
                },
                session,
                parameters,
                warningCollector,
                isDescribe,
                _ -> {
                    throw new IllegalStateException("Cannot access preanalyzed types");
                },
                _ -> {
                    throw new IllegalStateException("Cannot access resolved windows");
                });
    }

    public static void analyzeDefaultColumnValue(
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            WarningCollector warningCollector,
            Type columnType,
            Expression defaultLiteral)
    {
        if (!(defaultLiteral instanceof Literal literal)) {
            throw new IllegalArgumentException("Unsupported default expression: " + defaultLiteral);
        }

        try {
            ExpressionAnalyzer constantAnalyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, warningCollector);
            Type literalType = constantAnalyzer.analyze(literal, Scope.create());
            Object value = evaluateConstant(literal, literalType, parameters, plannerContext, session, accessControl);

            if (!literalType.equals(columnType)) {
                checkDefaultColumnValue(session, plannerContext, value, columnType, literalType);
            }
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_DEFAULT_COLUMN_VALUE, literal, e, "'%s' is not a valid %s literal", literal, columnType.getDisplayName().toUpperCase(ENGLISH));
        }
    }

    private static void checkDefaultColumnValue(Session session, PlannerContext plannerContext, Object value, Type type, Type literalType)
    {
        if (type instanceof BooleanType) {
            checkArgument(value instanceof Boolean, "Value must be true or false '%s'".formatted(value));
        }
        if (value == null) {
            return;
        }
        if (type instanceof CharType charType) {
            int targetLength = charType.getLength();
            Slice slice = (Slice) value;
            long actualLength = countCodePoints(slice);
            checkArgument(targetLength >= actualLength, "Cannot truncate characters when casting value '%s' to %s".formatted(slice.toStringUtf8(), type));
        }
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return;
            }
            int targetLength = varcharType.getBoundedLength();
            Slice slice = (Slice) value;
            long actualLength = countCodePoints(slice);
            checkArgument(targetLength >= actualLength, "Cannot truncate characters when casting value '%s' to %s".formatted(slice.toStringUtf8(), type));
        }
        if (type instanceof TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (value instanceof Long epochMicros) {
                int microOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
                checkArgument(microOfSecond % Math.pow(10, TIMESTAMP_MICROS.getPrecision() - Math.min(precision, TIMESTAMP_MICROS.getPrecision())) == 0, "Value too large");
            }
            else {
                LongTimestamp longTimestamp = (LongTimestamp) value;
                int picosOfMicro = longTimestamp.getPicosOfMicro();
                checkArgument(picosOfMicro % Math.pow(10, TIMESTAMP_PICOS.getPrecision() - precision) == 0, "Value too large");
            }
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            int precision = timestampWithTimeZoneType.getPrecision();
            if (value instanceof Long dateTimeWithTimeZone) {
                long epochMillis = unpackMillisUtc(dateTimeWithTimeZone);
                checkArgument(epochMillis % Math.pow(10, TIMESTAMP_TZ_MILLIS.getPrecision() - Math.min(precision, TIMESTAMP_TZ_MILLIS.getPrecision())) == 0, "Value too large");
            }
            else {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
                int picosOfMilli = timestamp.getPicosOfMilli();
                checkArgument(picosOfMilli % Math.pow(10, TIMESTAMP_TZ_PICOS.getPrecision() - precision) == 0, "Value too large");
            }
        }

        plannerContext.getExpressionEvaluator().evaluate(
                cast(plannerContext.getTypeManager(), getCharVarcharCoercion(session), new Constant(literalType, value), type),
                session,
                ImmutableMap.of());
    }

    public static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                type instanceof DecimalType ||
                type.equals(NUMBER);
    }

    private static boolean isExactNumericWithScaleZero(Type type)
    {
        return type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                (type instanceof DecimalType decimalType && decimalType.getScale() == 0);
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
