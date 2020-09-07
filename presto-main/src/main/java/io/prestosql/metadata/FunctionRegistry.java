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
package io.prestosql.metadata;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.operator.aggregation.ApproximateCountDistinctAggregation;
import io.prestosql.operator.aggregation.ApproximateDoublePercentileAggregations;
import io.prestosql.operator.aggregation.ApproximateDoublePercentileArrayAggregations;
import io.prestosql.operator.aggregation.ApproximateLongPercentileAggregations;
import io.prestosql.operator.aggregation.ApproximateLongPercentileArrayAggregations;
import io.prestosql.operator.aggregation.ApproximateRealPercentileAggregations;
import io.prestosql.operator.aggregation.ApproximateRealPercentileArrayAggregations;
import io.prestosql.operator.aggregation.ApproximateSetAggregation;
import io.prestosql.operator.aggregation.AverageAggregations;
import io.prestosql.operator.aggregation.BigintApproximateMostFrequent;
import io.prestosql.operator.aggregation.BitwiseAndAggregation;
import io.prestosql.operator.aggregation.BitwiseOrAggregation;
import io.prestosql.operator.aggregation.BooleanAndAggregation;
import io.prestosql.operator.aggregation.BooleanOrAggregation;
import io.prestosql.operator.aggregation.CentralMomentsAggregation;
import io.prestosql.operator.aggregation.CountAggregation;
import io.prestosql.operator.aggregation.CountIfAggregation;
import io.prestosql.operator.aggregation.DefaultApproximateCountDistinctAggregation;
import io.prestosql.operator.aggregation.DoubleCorrelationAggregation;
import io.prestosql.operator.aggregation.DoubleCovarianceAggregation;
import io.prestosql.operator.aggregation.DoubleHistogramAggregation;
import io.prestosql.operator.aggregation.DoubleRegressionAggregation;
import io.prestosql.operator.aggregation.DoubleSumAggregation;
import io.prestosql.operator.aggregation.GeometricMeanAggregations;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.IntervalDayToSecondAverageAggregation;
import io.prestosql.operator.aggregation.IntervalDayToSecondSumAggregation;
import io.prestosql.operator.aggregation.IntervalYearToMonthAverageAggregation;
import io.prestosql.operator.aggregation.IntervalYearToMonthSumAggregation;
import io.prestosql.operator.aggregation.LongSumAggregation;
import io.prestosql.operator.aggregation.MaxDataSizeForStats;
import io.prestosql.operator.aggregation.MergeHyperLogLogAggregation;
import io.prestosql.operator.aggregation.MergeQuantileDigestFunction;
import io.prestosql.operator.aggregation.RealCorrelationAggregation;
import io.prestosql.operator.aggregation.RealCovarianceAggregation;
import io.prestosql.operator.aggregation.RealGeometricMeanAggregations;
import io.prestosql.operator.aggregation.RealHistogramAggregation;
import io.prestosql.operator.aggregation.RealRegressionAggregation;
import io.prestosql.operator.aggregation.RealSumAggregation;
import io.prestosql.operator.aggregation.SumDataSizeForStats;
import io.prestosql.operator.aggregation.VarcharApproximateMostFrequent;
import io.prestosql.operator.aggregation.VarianceAggregation;
import io.prestosql.operator.aggregation.arrayagg.ArrayAggregationFunction;
import io.prestosql.operator.aggregation.histogram.Histogram;
import io.prestosql.operator.aggregation.multimapagg.MultimapAggregationFunction;
import io.prestosql.operator.scalar.ArrayAllMatchFunction;
import io.prestosql.operator.scalar.ArrayAnyMatchFunction;
import io.prestosql.operator.scalar.ArrayCardinalityFunction;
import io.prestosql.operator.scalar.ArrayCombinationsFunction;
import io.prestosql.operator.scalar.ArrayContains;
import io.prestosql.operator.scalar.ArrayDistinctFromOperator;
import io.prestosql.operator.scalar.ArrayDistinctFunction;
import io.prestosql.operator.scalar.ArrayElementAtFunction;
import io.prestosql.operator.scalar.ArrayEqualOperator;
import io.prestosql.operator.scalar.ArrayExceptFunction;
import io.prestosql.operator.scalar.ArrayFilterFunction;
import io.prestosql.operator.scalar.ArrayFunctions;
import io.prestosql.operator.scalar.ArrayGreaterThanOperator;
import io.prestosql.operator.scalar.ArrayGreaterThanOrEqualOperator;
import io.prestosql.operator.scalar.ArrayHashCodeOperator;
import io.prestosql.operator.scalar.ArrayIndeterminateOperator;
import io.prestosql.operator.scalar.ArrayIntersectFunction;
import io.prestosql.operator.scalar.ArrayLessThanOperator;
import io.prestosql.operator.scalar.ArrayLessThanOrEqualOperator;
import io.prestosql.operator.scalar.ArrayMaxFunction;
import io.prestosql.operator.scalar.ArrayMinFunction;
import io.prestosql.operator.scalar.ArrayNgramsFunction;
import io.prestosql.operator.scalar.ArrayNoneMatchFunction;
import io.prestosql.operator.scalar.ArrayNotEqualOperator;
import io.prestosql.operator.scalar.ArrayPositionFunction;
import io.prestosql.operator.scalar.ArrayRemoveFunction;
import io.prestosql.operator.scalar.ArrayReverseFunction;
import io.prestosql.operator.scalar.ArrayShuffleFunction;
import io.prestosql.operator.scalar.ArraySliceFunction;
import io.prestosql.operator.scalar.ArraySortComparatorFunction;
import io.prestosql.operator.scalar.ArraySortFunction;
import io.prestosql.operator.scalar.ArrayUnionFunction;
import io.prestosql.operator.scalar.ArraysOverlapFunction;
import io.prestosql.operator.scalar.BitwiseFunctions;
import io.prestosql.operator.scalar.CharacterStringCasts;
import io.prestosql.operator.scalar.ColorFunctions;
import io.prestosql.operator.scalar.CombineHashFunction;
import io.prestosql.operator.scalar.DataSizeFunctions;
import io.prestosql.operator.scalar.DateTimeFunctions;
import io.prestosql.operator.scalar.EmptyMapConstructor;
import io.prestosql.operator.scalar.FailureFunction;
import io.prestosql.operator.scalar.HmacFunctions;
import io.prestosql.operator.scalar.HyperLogLogFunctions;
import io.prestosql.operator.scalar.JoniRegexpCasts;
import io.prestosql.operator.scalar.JoniRegexpFunctions;
import io.prestosql.operator.scalar.JoniRegexpReplaceLambdaFunction;
import io.prestosql.operator.scalar.JsonFunctions;
import io.prestosql.operator.scalar.JsonOperators;
import io.prestosql.operator.scalar.LuhnCheckFunction;
import io.prestosql.operator.scalar.MapCardinalityFunction;
import io.prestosql.operator.scalar.MapDistinctFromOperator;
import io.prestosql.operator.scalar.MapEntriesFunction;
import io.prestosql.operator.scalar.MapEqualOperator;
import io.prestosql.operator.scalar.MapFromEntriesFunction;
import io.prestosql.operator.scalar.MapIndeterminateOperator;
import io.prestosql.operator.scalar.MapKeys;
import io.prestosql.operator.scalar.MapNotEqualOperator;
import io.prestosql.operator.scalar.MapSubscriptOperator;
import io.prestosql.operator.scalar.MapValues;
import io.prestosql.operator.scalar.MathFunctions;
import io.prestosql.operator.scalar.MultimapFromEntriesFunction;
import io.prestosql.operator.scalar.QuantileDigestFunctions;
import io.prestosql.operator.scalar.Re2JRegexpFunctions;
import io.prestosql.operator.scalar.Re2JRegexpReplaceLambdaFunction;
import io.prestosql.operator.scalar.RepeatFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.scalar.SequenceFunction;
import io.prestosql.operator.scalar.SessionFunctions;
import io.prestosql.operator.scalar.SplitToMapFunction;
import io.prestosql.operator.scalar.SplitToMultimapFunction;
import io.prestosql.operator.scalar.StringFunctions;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.operator.scalar.TypeOfFunction;
import io.prestosql.operator.scalar.UrlFunctions;
import io.prestosql.operator.scalar.VarbinaryFunctions;
import io.prestosql.operator.scalar.WilsonInterval;
import io.prestosql.operator.scalar.WordStemFunction;
import io.prestosql.operator.scalar.time.LocalTimeFunction;
import io.prestosql.operator.scalar.time.TimeFunctions;
import io.prestosql.operator.scalar.time.TimeOperators;
import io.prestosql.operator.scalar.time.TimeToTimeWithTimeZoneCast;
import io.prestosql.operator.scalar.time.TimeToTimestampCast;
import io.prestosql.operator.scalar.time.TimeToTimestampWithTimeZoneCast;
import io.prestosql.operator.scalar.timestamp.DateAdd;
import io.prestosql.operator.scalar.timestamp.DateDiff;
import io.prestosql.operator.scalar.timestamp.DateFormat;
import io.prestosql.operator.scalar.timestamp.DateToTimestampCast;
import io.prestosql.operator.scalar.timestamp.DateTrunc;
import io.prestosql.operator.scalar.timestamp.ExtractDay;
import io.prestosql.operator.scalar.timestamp.ExtractDayOfWeek;
import io.prestosql.operator.scalar.timestamp.ExtractDayOfYear;
import io.prestosql.operator.scalar.timestamp.ExtractHour;
import io.prestosql.operator.scalar.timestamp.ExtractMillisecond;
import io.prestosql.operator.scalar.timestamp.ExtractMinute;
import io.prestosql.operator.scalar.timestamp.ExtractMonth;
import io.prestosql.operator.scalar.timestamp.ExtractQuarter;
import io.prestosql.operator.scalar.timestamp.ExtractSecond;
import io.prestosql.operator.scalar.timestamp.ExtractWeekOfYear;
import io.prestosql.operator.scalar.timestamp.ExtractYear;
import io.prestosql.operator.scalar.timestamp.ExtractYearOfWeek;
import io.prestosql.operator.scalar.timestamp.FormatDateTime;
import io.prestosql.operator.scalar.timestamp.HumanReadableSeconds;
import io.prestosql.operator.scalar.timestamp.LastDayOfMonth;
import io.prestosql.operator.scalar.timestamp.LocalTimestamp;
import io.prestosql.operator.scalar.timestamp.SequenceIntervalDayToSecond;
import io.prestosql.operator.scalar.timestamp.SequenceIntervalYearToMonth;
import io.prestosql.operator.scalar.timestamp.TimeWithTimezoneToTimestampCast;
import io.prestosql.operator.scalar.timestamp.TimestampDistinctFromOperator;
import io.prestosql.operator.scalar.timestamp.TimestampOperators;
import io.prestosql.operator.scalar.timestamp.TimestampToDateCast;
import io.prestosql.operator.scalar.timestamp.TimestampToJsonCast;
import io.prestosql.operator.scalar.timestamp.TimestampToTimeCast;
import io.prestosql.operator.scalar.timestamp.TimestampToTimeWithTimezoneCast;
import io.prestosql.operator.scalar.timestamp.TimestampToTimestampCast;
import io.prestosql.operator.scalar.timestamp.TimestampToTimestampWithTimezoneCast;
import io.prestosql.operator.scalar.timestamp.TimestampToVarcharCast;
import io.prestosql.operator.scalar.timestamp.ToIso8601;
import io.prestosql.operator.scalar.timestamp.ToUnixTime;
import io.prestosql.operator.scalar.timestamp.VarcharToTimestampCast;
import io.prestosql.operator.scalar.timestamp.WithTimeZone;
import io.prestosql.operator.scalar.timestamptz.AtTimeZone;
import io.prestosql.operator.scalar.timestamptz.AtTimeZoneWithOffset;
import io.prestosql.operator.scalar.timestamptz.CurrentTimestamp;
import io.prestosql.operator.scalar.timestamptz.DateToTimestampWithTimeZoneCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneDistinctFromOperator;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneOperators;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneToDateCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneToTimeCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneToTimeWithTimezoneCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneToTimestampWithTimeZoneCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneToVarcharCast;
import io.prestosql.operator.scalar.timestamptz.TimestampWithTimezoneToTimestampCast;
import io.prestosql.operator.scalar.timestamptz.VarcharToTimestampWithTimeZoneCast;
import io.prestosql.operator.scalar.timetz.CurrentTime;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneDistinctFromOperator;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneOperators;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneToTimeCast;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneToTimeWithTimeZoneCast;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneToTimestampWithTimeZoneCast;
import io.prestosql.operator.scalar.timetz.TimeWithTimeZoneToVarcharCast;
import io.prestosql.operator.scalar.timetz.VarcharToTimeWithTimeZoneCast;
import io.prestosql.operator.window.CumulativeDistributionFunction;
import io.prestosql.operator.window.DenseRankFunction;
import io.prestosql.operator.window.FirstValueFunction;
import io.prestosql.operator.window.LagFunction;
import io.prestosql.operator.window.LastValueFunction;
import io.prestosql.operator.window.LeadFunction;
import io.prestosql.operator.window.NTileFunction;
import io.prestosql.operator.window.NthValueFunction;
import io.prestosql.operator.window.PercentRankFunction;
import io.prestosql.operator.window.RankFunction;
import io.prestosql.operator.window.RowNumberFunction;
import io.prestosql.operator.window.SqlWindowFunction;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.type.BigintOperators;
import io.prestosql.type.BooleanOperators;
import io.prestosql.type.CharOperators;
import io.prestosql.type.ColorOperators;
import io.prestosql.type.DateOperators;
import io.prestosql.type.DateTimeOperators;
import io.prestosql.type.DecimalOperators;
import io.prestosql.type.DoubleOperators;
import io.prestosql.type.HyperLogLogOperators;
import io.prestosql.type.IntegerOperators;
import io.prestosql.type.IntervalDayTimeOperators;
import io.prestosql.type.IntervalYearMonthOperators;
import io.prestosql.type.IpAddressOperators;
import io.prestosql.type.LikeFunctions;
import io.prestosql.type.QuantileDigestOperators;
import io.prestosql.type.RealOperators;
import io.prestosql.type.SmallintOperators;
import io.prestosql.type.TinyintOperators;
import io.prestosql.type.UnknownOperators;
import io.prestosql.type.UuidOperators;
import io.prestosql.type.VarbinaryOperators;
import io.prestosql.type.VarcharOperators;
import io.prestosql.type.setdigest.BuildSetDigestAggregation;
import io.prestosql.type.setdigest.MergeSetDigestAggregation;
import io.prestosql.type.setdigest.SetDigestFunctions;
import io.prestosql.type.setdigest.SetDigestOperators;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.operator.aggregation.ArbitraryAggregationFunction.ARBITRARY_AGGREGATION;
import static io.prestosql.operator.aggregation.ChecksumAggregationFunction.CHECKSUM_AGGREGATION;
import static io.prestosql.operator.aggregation.CountColumn.COUNT_COLUMN;
import static io.prestosql.operator.aggregation.DecimalAverageAggregation.DECIMAL_AVERAGE_AGGREGATION;
import static io.prestosql.operator.aggregation.DecimalSumAggregation.DECIMAL_SUM_AGGREGATION;
import static io.prestosql.operator.aggregation.MapAggregationFunction.MAP_AGG;
import static io.prestosql.operator.aggregation.MapUnionAggregation.MAP_UNION;
import static io.prestosql.operator.aggregation.MaxAggregationFunction.MAX_AGGREGATION;
import static io.prestosql.operator.aggregation.MaxNAggregationFunction.MAX_N_AGGREGATION;
import static io.prestosql.operator.aggregation.MinAggregationFunction.MIN_AGGREGATION;
import static io.prestosql.operator.aggregation.MinNAggregationFunction.MIN_N_AGGREGATION;
import static io.prestosql.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG;
import static io.prestosql.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG_WITH_WEIGHT;
import static io.prestosql.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG_WITH_WEIGHT_AND_ERROR;
import static io.prestosql.operator.aggregation.RealAverageAggregation.REAL_AVERAGE_AGGREGATION;
import static io.prestosql.operator.aggregation.ReduceAggregationFunction.REDUCE_AGG;
import static io.prestosql.operator.aggregation.minmaxby.MaxByAggregationFunction.MAX_BY;
import static io.prestosql.operator.aggregation.minmaxby.MaxByNAggregationFunction.MAX_BY_N_AGGREGATION;
import static io.prestosql.operator.aggregation.minmaxby.MinByAggregationFunction.MIN_BY;
import static io.prestosql.operator.aggregation.minmaxby.MinByNAggregationFunction.MIN_BY_N_AGGREGATION;
import static io.prestosql.operator.scalar.ArrayConcatFunction.ARRAY_CONCAT_FUNCTION;
import static io.prestosql.operator.scalar.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static io.prestosql.operator.scalar.ArrayFlattenFunction.ARRAY_FLATTEN_FUNCTION;
import static io.prestosql.operator.scalar.ArrayJoin.ARRAY_JOIN;
import static io.prestosql.operator.scalar.ArrayJoin.ARRAY_JOIN_WITH_NULL_REPLACEMENT;
import static io.prestosql.operator.scalar.ArrayReduceFunction.ARRAY_REDUCE_FUNCTION;
import static io.prestosql.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static io.prestosql.operator.scalar.ArrayToArrayCast.ARRAY_TO_ARRAY_CAST;
import static io.prestosql.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static io.prestosql.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static io.prestosql.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_FUNCTION;
import static io.prestosql.operator.scalar.CastFromUnknownOperator.CAST_FROM_UNKNOWN;
import static io.prestosql.operator.scalar.ConcatFunction.VARBINARY_CONCAT;
import static io.prestosql.operator.scalar.ConcatFunction.VARCHAR_CONCAT;
import static io.prestosql.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static io.prestosql.operator.scalar.FormatFunction.FORMAT_FUNCTION;
import static io.prestosql.operator.scalar.Greatest.GREATEST;
import static io.prestosql.operator.scalar.IdentityCast.IDENTITY_CAST;
import static io.prestosql.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY;
import static io.prestosql.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP;
import static io.prestosql.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW;
import static io.prestosql.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static io.prestosql.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static io.prestosql.operator.scalar.JsonToRowCast.JSON_TO_ROW;
import static io.prestosql.operator.scalar.Least.LEAST;
import static io.prestosql.operator.scalar.MapConcatFunction.MAP_CONCAT_FUNCTION;
import static io.prestosql.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static io.prestosql.operator.scalar.MapElementAtFunction.MAP_ELEMENT_AT;
import static io.prestosql.operator.scalar.MapFilterFunction.MAP_FILTER_FUNCTION;
import static io.prestosql.operator.scalar.MapHashCodeOperator.MAP_HASH_CODE;
import static io.prestosql.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static io.prestosql.operator.scalar.MapToMapCast.MAP_TO_MAP_CAST;
import static io.prestosql.operator.scalar.MapTransformKeysFunction.MAP_TRANSFORM_KEYS_FUNCTION;
import static io.prestosql.operator.scalar.MapTransformValuesFunction.MAP_TRANSFORM_VALUES_FUNCTION;
import static io.prestosql.operator.scalar.MapZipWithFunction.MAP_ZIP_WITH_FUNCTION;
import static io.prestosql.operator.scalar.MathFunctions.DECIMAL_MOD_FUNCTION;
import static io.prestosql.operator.scalar.Re2JCastToRegexpFunction.castCharToRe2JRegexp;
import static io.prestosql.operator.scalar.Re2JCastToRegexpFunction.castVarcharToRe2JRegexp;
import static io.prestosql.operator.scalar.RowDistinctFromOperator.ROW_DISTINCT_FROM;
import static io.prestosql.operator.scalar.RowEqualOperator.ROW_EQUAL;
import static io.prestosql.operator.scalar.RowGreaterThanOperator.ROW_GREATER_THAN;
import static io.prestosql.operator.scalar.RowGreaterThanOrEqualOperator.ROW_GREATER_THAN_OR_EQUAL;
import static io.prestosql.operator.scalar.RowHashCodeOperator.ROW_HASH_CODE;
import static io.prestosql.operator.scalar.RowIndeterminateOperator.ROW_INDETERMINATE;
import static io.prestosql.operator.scalar.RowLessThanOperator.ROW_LESS_THAN;
import static io.prestosql.operator.scalar.RowLessThanOrEqualOperator.ROW_LESS_THAN_OR_EQUAL;
import static io.prestosql.operator.scalar.RowNotEqualOperator.ROW_NOT_EQUAL;
import static io.prestosql.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static io.prestosql.operator.scalar.RowToRowCast.ROW_TO_ROW_CAST;
import static io.prestosql.operator.scalar.TryCastFunction.TRY_CAST;
import static io.prestosql.operator.scalar.ZipFunction.ZIP_FUNCTIONS;
import static io.prestosql.operator.scalar.ZipWithFunction.ZIP_WITH_FUNCTION;
import static io.prestosql.operator.window.AggregateWindowFunction.supplier;
import static io.prestosql.type.DecimalCasts.BIGINT_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.BOOLEAN_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_BIGINT_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_BOOLEAN_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_DOUBLE_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_INTEGER_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_JSON_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_REAL_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_SMALLINT_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_TINYINT_CAST;
import static io.prestosql.type.DecimalCasts.DECIMAL_TO_VARCHAR_CAST;
import static io.prestosql.type.DecimalCasts.DOUBLE_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.INTEGER_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.JSON_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.REAL_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.SMALLINT_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.TINYINT_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalCasts.VARCHAR_TO_DECIMAL_CAST;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_DISTINCT_FROM_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_EQUAL_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR;
import static io.prestosql.type.DecimalInequalityOperators.DECIMAL_NOT_EQUAL_OPERATOR;
import static io.prestosql.type.DecimalOperators.DECIMAL_ADD_OPERATOR;
import static io.prestosql.type.DecimalOperators.DECIMAL_DIVIDE_OPERATOR;
import static io.prestosql.type.DecimalOperators.DECIMAL_MODULUS_OPERATOR;
import static io.prestosql.type.DecimalOperators.DECIMAL_MULTIPLY_OPERATOR;
import static io.prestosql.type.DecimalOperators.DECIMAL_SUBTRACT_OPERATOR;
import static io.prestosql.type.DecimalSaturatedFloorCasts.BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalSaturatedFloorCasts.TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.prestosql.type.DecimalToDecimalCasts.DECIMAL_TO_DECIMAL_CAST;
import static java.util.concurrent.TimeUnit.HOURS;

@ThreadSafe
public class FunctionRegistry
{
    private final Cache<FunctionBinding, ScalarFunctionImplementation> specializedScalarCache;
    private final Cache<FunctionBinding, InternalAggregationFunction> specializedAggregationCache;
    private final Cache<FunctionBinding, WindowFunctionSupplier> specializedWindowCache;
    private volatile FunctionMap functions = new FunctionMap();

    public FunctionRegistry(Supplier<BlockEncodingSerde> blockEncodingSerdeSupplier, FeaturesConfig featuresConfig)
    {
        // We have observed repeated compilation of MethodHandle that leads to full GCs.
        // We notice that flushing the following caches mitigate the problem.
        // We suspect that it is a JVM bug that is related to stale/corrupted profiling data associated
        // with generated classes and/or dynamically-created MethodHandles.
        // This might also mitigate problems like deoptimization storm or unintended interpreted execution.

        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build();

        specializedAggregationCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build();

        specializedWindowCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build();

        FunctionListBuilder builder = new FunctionListBuilder()
                .window(RowNumberFunction.class)
                .window(RankFunction.class)
                .window(DenseRankFunction.class)
                .window(PercentRankFunction.class)
                .window(CumulativeDistributionFunction.class)
                .window(NTileFunction.class)
                .window(FirstValueFunction.class)
                .window(LastValueFunction.class)
                .window(NthValueFunction.class)
                .window(LagFunction.class)
                .window(LeadFunction.class)
                .aggregate(ApproximateCountDistinctAggregation.class)
                .aggregate(DefaultApproximateCountDistinctAggregation.class)
                .aggregate(SumDataSizeForStats.class)
                .aggregate(MaxDataSizeForStats.class)
                .aggregates(CountAggregation.class)
                .aggregates(VarianceAggregation.class)
                .aggregates(CentralMomentsAggregation.class)
                .aggregates(ApproximateLongPercentileAggregations.class)
                .aggregates(ApproximateLongPercentileArrayAggregations.class)
                .aggregates(ApproximateDoublePercentileAggregations.class)
                .aggregates(ApproximateDoublePercentileArrayAggregations.class)
                .aggregates(ApproximateRealPercentileAggregations.class)
                .aggregates(ApproximateRealPercentileArrayAggregations.class)
                .aggregates(CountIfAggregation.class)
                .aggregates(BooleanAndAggregation.class)
                .aggregates(BooleanOrAggregation.class)
                .aggregates(DoubleSumAggregation.class)
                .aggregates(RealSumAggregation.class)
                .aggregates(LongSumAggregation.class)
                .aggregates(IntervalDayToSecondSumAggregation.class)
                .aggregates(IntervalYearToMonthSumAggregation.class)
                .aggregates(AverageAggregations.class)
                .function(REAL_AVERAGE_AGGREGATION)
                .aggregates(IntervalDayToSecondAverageAggregation.class)
                .aggregates(IntervalYearToMonthAverageAggregation.class)
                .aggregates(GeometricMeanAggregations.class)
                .aggregates(RealGeometricMeanAggregations.class)
                .aggregates(MergeHyperLogLogAggregation.class)
                .aggregates(ApproximateSetAggregation.class)
                .functions(QDIGEST_AGG, QDIGEST_AGG_WITH_WEIGHT, QDIGEST_AGG_WITH_WEIGHT_AND_ERROR)
                .function(MergeQuantileDigestFunction.MERGE)
                .aggregates(DoubleHistogramAggregation.class)
                .aggregates(RealHistogramAggregation.class)
                .aggregates(DoubleCovarianceAggregation.class)
                .aggregates(RealCovarianceAggregation.class)
                .aggregates(DoubleRegressionAggregation.class)
                .aggregates(RealRegressionAggregation.class)
                .aggregates(DoubleCorrelationAggregation.class)
                .aggregates(RealCorrelationAggregation.class)
                .aggregates(BitwiseOrAggregation.class)
                .aggregates(BitwiseAndAggregation.class)
                .scalar(RepeatFunction.class)
                .scalars(SequenceFunction.class)
                .scalars(SessionFunctions.class)
                .scalars(StringFunctions.class)
                .scalars(WordStemFunction.class)
                .scalar(SplitToMapFunction.class)
                .scalar(SplitToMultimapFunction.class)
                .scalars(VarbinaryFunctions.class)
                .scalars(UrlFunctions.class)
                .scalars(MathFunctions.class)
                .scalar(MathFunctions.Abs.class)
                .scalar(MathFunctions.Sign.class)
                .scalar(MathFunctions.Round.class)
                .scalar(MathFunctions.RoundN.class)
                .scalar(MathFunctions.Truncate.class)
                .scalar(MathFunctions.TruncateN.class)
                .scalar(MathFunctions.Ceiling.class)
                .scalar(MathFunctions.Floor.class)
                .scalars(BitwiseFunctions.class)
                .scalars(DateTimeFunctions.class)
                .scalars(JsonFunctions.class)
                .scalars(ColorFunctions.class)
                .scalars(ColorOperators.class)
                .scalar(ColorOperators.ColorDistinctFromOperator.class)
                .scalars(HyperLogLogFunctions.class)
                .scalars(QuantileDigestFunctions.class)
                .scalars(UnknownOperators.class)
                .scalar(UnknownOperators.UnknownDistinctFromOperator.class)
                .scalars(BooleanOperators.class)
                .scalar(BooleanOperators.BooleanDistinctFromOperator.class)
                .scalars(BigintOperators.class)
                .scalar(BigintOperators.BigintDistinctFromOperator.class)
                .scalars(IntegerOperators.class)
                .scalar(IntegerOperators.IntegerDistinctFromOperator.class)
                .scalars(SmallintOperators.class)
                .scalar(SmallintOperators.SmallintDistinctFromOperator.class)
                .scalars(TinyintOperators.class)
                .scalar(TinyintOperators.TinyintDistinctFromOperator.class)
                .scalars(DoubleOperators.class)
                .scalar(DoubleOperators.DoubleDistinctFromOperator.class)
                .scalars(RealOperators.class)
                .scalar(RealOperators.RealDistinctFromOperator.class)
                .scalars(VarcharOperators.class)
                .scalar(VarcharOperators.VarcharDistinctFromOperator.class)
                .scalars(VarbinaryOperators.class)
                .scalar(VarbinaryOperators.VarbinaryDistinctFromOperator.class)
                .scalars(DateOperators.class)
                .scalar(DateOperators.DateDistinctFromOperator.class)
                .scalars(IntervalDayTimeOperators.class)
                .scalar(IntervalDayTimeOperators.IntervalDayTimeDistinctFromOperator.class)
                .scalars(IntervalYearMonthOperators.class)
                .scalar(IntervalYearMonthOperators.IntervalYearMonthDistinctFromOperator.class)
                .scalars(DateTimeOperators.class)
                .scalars(HyperLogLogOperators.class)
                .scalars(QuantileDigestOperators.class)
                .scalars(IpAddressOperators.class)
                .scalar(IpAddressOperators.IpAddressDistinctFromOperator.class)
                .scalars(UuidOperators.class)
                .scalar(UuidOperators.UuidDistinctFromOperator.class)
                .scalars(LikeFunctions.class)
                .scalars(ArrayFunctions.class)
                .scalars(HmacFunctions.class)
                .scalars(DataSizeFunctions.class)
                .scalar(ArrayCardinalityFunction.class)
                .scalar(ArrayContains.class)
                .scalar(ArrayFilterFunction.class)
                .scalar(ArrayPositionFunction.class)
                .scalars(CombineHashFunction.class)
                .scalars(JsonOperators.class)
                .scalar(JsonOperators.JsonDistinctFromOperator.class)
                .scalars(FailureFunction.class)
                .scalars(JoniRegexpCasts.class)
                .scalars(CharacterStringCasts.class)
                .scalars(CharOperators.class)
                .scalars(LuhnCheckFunction.class)
                .scalar(CharOperators.CharDistinctFromOperator.class)
                .scalar(DecimalOperators.Negation.class)
                .scalar(DecimalOperators.HashCode.class)
                .scalar(DecimalOperators.Indeterminate.class)
                .scalar(DecimalOperators.XxHash64Operator.class)
                .functions(IDENTITY_CAST, CAST_FROM_UNKNOWN)
                .scalar(ArrayLessThanOperator.class)
                .scalar(ArrayLessThanOrEqualOperator.class)
                .scalar(ArrayRemoveFunction.class)
                .scalar(ArrayGreaterThanOperator.class)
                .scalar(ArrayGreaterThanOrEqualOperator.class)
                .scalar(ArrayElementAtFunction.class)
                .scalar(ArraySortFunction.class)
                .scalar(ArraySortComparatorFunction.class)
                .scalar(ArrayShuffleFunction.class)
                .scalar(ArrayReverseFunction.class)
                .scalar(ArrayMinFunction.class)
                .scalar(ArrayMaxFunction.class)
                .scalar(ArrayDistinctFunction.class)
                .scalar(ArrayNotEqualOperator.class)
                .scalar(ArrayEqualOperator.class)
                .scalar(ArrayHashCodeOperator.class)
                .scalar(ArrayIntersectFunction.class)
                .scalar(ArraysOverlapFunction.class)
                .scalar(ArrayDistinctFromOperator.class)
                .scalar(ArrayUnionFunction.class)
                .scalar(ArrayExceptFunction.class)
                .scalar(ArraySliceFunction.class)
                .scalar(ArrayIndeterminateOperator.class)
                .scalar(ArrayCombinationsFunction.class)
                .scalar(ArrayNgramsFunction.class)
                .scalar(ArrayAllMatchFunction.class)
                .scalar(ArrayAnyMatchFunction.class)
                .scalar(ArrayNoneMatchFunction.class)
                .scalar(MapDistinctFromOperator.class)
                .scalar(MapEqualOperator.class)
                .scalar(MapEntriesFunction.class)
                .scalar(MapFromEntriesFunction.class)
                .scalar(MultimapFromEntriesFunction.class)
                .scalar(MapNotEqualOperator.class)
                .scalar(MapKeys.class)
                .scalar(MapValues.class)
                .scalar(MapCardinalityFunction.class)
                .scalar(EmptyMapConstructor.class)
                .scalar(MapIndeterminateOperator.class)
                .scalar(TypeOfFunction.class)
                .scalar(TryFunction.class)
                .scalar(DynamicFilters.Function.class)
                .functions(ZIP_WITH_FUNCTION, MAP_ZIP_WITH_FUNCTION)
                .functions(ZIP_FUNCTIONS)
                .functions(ARRAY_JOIN, ARRAY_JOIN_WITH_NULL_REPLACEMENT)
                .functions(ARRAY_TO_ARRAY_CAST)
                .functions(ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .function(MAP_HASH_CODE)
                .function(MAP_ELEMENT_AT)
                .function(MAP_CONCAT_FUNCTION)
                .function(MAP_TO_MAP_CAST)
                .function(ARRAY_FLATTEN_FUNCTION)
                .function(ARRAY_CONCAT_FUNCTION)
                .functions(ARRAY_CONSTRUCTOR, ARRAY_SUBSCRIPT, ARRAY_TO_JSON, JSON_TO_ARRAY, JSON_STRING_TO_ARRAY)
                .function(new ArrayAggregationFunction(featuresConfig.getArrayAggGroupImplementation()))
                .functions(new MapSubscriptOperator())
                .functions(MAP_CONSTRUCTOR, MAP_TO_JSON, JSON_TO_MAP, JSON_STRING_TO_MAP)
                .functions(MAP_AGG, MAP_UNION)
                .function(REDUCE_AGG)
                .function(new MultimapAggregationFunction(featuresConfig.getMultimapAggGroupImplementation()))
                .functions(DECIMAL_TO_VARCHAR_CAST, DECIMAL_TO_INTEGER_CAST, DECIMAL_TO_BIGINT_CAST, DECIMAL_TO_DOUBLE_CAST, DECIMAL_TO_REAL_CAST, DECIMAL_TO_BOOLEAN_CAST, DECIMAL_TO_TINYINT_CAST, DECIMAL_TO_SMALLINT_CAST)
                .functions(VARCHAR_TO_DECIMAL_CAST, INTEGER_TO_DECIMAL_CAST, BIGINT_TO_DECIMAL_CAST, DOUBLE_TO_DECIMAL_CAST, REAL_TO_DECIMAL_CAST, BOOLEAN_TO_DECIMAL_CAST, TINYINT_TO_DECIMAL_CAST, SMALLINT_TO_DECIMAL_CAST)
                .functions(JSON_TO_DECIMAL_CAST, DECIMAL_TO_JSON_CAST)
                .functions(DECIMAL_ADD_OPERATOR, DECIMAL_SUBTRACT_OPERATOR, DECIMAL_MULTIPLY_OPERATOR, DECIMAL_DIVIDE_OPERATOR, DECIMAL_MODULUS_OPERATOR)
                .functions(DECIMAL_EQUAL_OPERATOR, DECIMAL_NOT_EQUAL_OPERATOR)
                .functions(DECIMAL_LESS_THAN_OPERATOR, DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR)
                .functions(DECIMAL_GREATER_THAN_OPERATOR, DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR)
                .function(DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST, BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST, INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST, SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST, TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .function(DECIMAL_DISTINCT_FROM_OPERATOR)
                .function(new Histogram(featuresConfig.getHistogramGroupImplementation()))
                .function(CHECKSUM_AGGREGATION)
                .function(ARBITRARY_AGGREGATION)
                .functions(GREATEST, LEAST)
                .functions(MAX_BY, MIN_BY, MAX_BY_N_AGGREGATION, MIN_BY_N_AGGREGATION)
                .functions(MAX_AGGREGATION, MIN_AGGREGATION, MAX_N_AGGREGATION, MIN_N_AGGREGATION)
                .function(COUNT_COLUMN)
                .functions(ROW_HASH_CODE, ROW_TO_JSON, JSON_TO_ROW, JSON_STRING_TO_ROW, ROW_DISTINCT_FROM, ROW_EQUAL, ROW_GREATER_THAN, ROW_GREATER_THAN_OR_EQUAL, ROW_LESS_THAN, ROW_LESS_THAN_OR_EQUAL, ROW_NOT_EQUAL, ROW_TO_ROW_CAST, ROW_INDETERMINATE)
                .functions(VARCHAR_CONCAT, VARBINARY_CONCAT)
                .function(DECIMAL_TO_DECIMAL_CAST)
                .function(castVarcharToRe2JRegexp(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()))
                .function(castCharToRe2JRegexp(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()))
                .function(DECIMAL_AVERAGE_AGGREGATION)
                .function(DECIMAL_SUM_AGGREGATION)
                .function(DECIMAL_MOD_FUNCTION)
                .functions(ARRAY_TRANSFORM_FUNCTION, ARRAY_REDUCE_FUNCTION)
                .functions(MAP_FILTER_FUNCTION, MAP_TRANSFORM_KEYS_FUNCTION, MAP_TRANSFORM_VALUES_FUNCTION)
                .function(FORMAT_FUNCTION)
                .function(TRY_CAST)
                .function(new LiteralFunction(blockEncodingSerdeSupplier))
                .aggregate(MergeSetDigestAggregation.class)
                .aggregate(BuildSetDigestAggregation.class)
                .scalars(SetDigestFunctions.class)
                .scalars(SetDigestOperators.class)
                .scalars(WilsonInterval.class)
                .aggregate(BigintApproximateMostFrequent.class)
                .aggregate(VarcharApproximateMostFrequent.class);

        // timestamp operators and functions
        builder
                .scalar(TimestampOperators.Equal.class)
                .scalar(TimestampOperators.NotEqual.class)
                .scalar(TimestampOperators.LessThan.class)
                .scalar(TimestampOperators.LessThanOrEqual.class)
                .scalar(TimestampOperators.GreaterThan.class)
                .scalar(TimestampOperators.GreaterThanOrEqual.class)
                .scalar(TimestampDistinctFromOperator.class)
                .scalar(TimestampOperators.HashCode.class)
                .scalar(TimestampOperators.Indeterminate.class)
                .scalar(TimestampOperators.XxHash64Operator.class)
                .scalar(TimestampOperators.TimestampPlusIntervalDayToSecond.class)
                .scalar(TimestampOperators.IntervalDayToSecondPlusTimestamp.class)
                .scalar(TimestampOperators.TimestampPlusIntervalYearToMonth.class)
                .scalar(TimestampOperators.IntervalYearToMonthPlusTimestamp.class)
                .scalar(TimestampOperators.TimestampMinusIntervalDayToSecond.class)
                .scalar(TimestampOperators.TimestampMinusIntervalYearToMonth.class)
                .scalar(TimestampOperators.TimestampMinusTimestamp.class)
                .scalar(TimestampToTimestampCast.class)
                .scalar(TimestampToTimeCast.class)
                .scalar(TimestampToTimeWithTimezoneCast.class)
                .scalar(TimestampToTimestampWithTimezoneCast.class)
                .scalar(TimestampToDateCast.class)
                .scalar(TimestampToVarcharCast.class)
                .scalar(TimestampToJsonCast.class)
                .scalar(DateToTimestampCast.class)
                .scalar(TimeToTimestampCast.class)
                .scalar(TimeWithTimezoneToTimestampCast.class)
                .scalar(TimestampWithTimezoneToTimestampCast.class)
                .scalar(VarcharToTimestampCast.class)
                .scalar(LocalTimestamp.class)
                .scalar(DateTrunc.class)
                .scalar(ToUnixTime.class)
                .scalar(HumanReadableSeconds.class)
                .scalar(ToIso8601.class)
                .scalar(WithTimeZone.class)
                .scalar(FormatDateTime.class)
                .scalar(DateFormat.class)
                .scalar(SequenceIntervalYearToMonth.class)
                .scalar(SequenceIntervalDayToSecond.class)
                .scalar(DateAdd.class)
                .scalar(DateDiff.class)
                .scalar(ExtractYear.class)
                .scalar(ExtractQuarter.class)
                .scalar(ExtractMonth.class)
                .scalar(ExtractDay.class)
                .scalar(ExtractHour.class)
                .scalar(ExtractMinute.class)
                .scalar(ExtractSecond.class)
                .scalar(ExtractMillisecond.class)
                .scalar(ExtractDayOfYear.class)
                .scalar(ExtractDayOfWeek.class)
                .scalar(ExtractWeekOfYear.class)
                .scalar(ExtractYearOfWeek.class)
                .scalar(LastDayOfMonth.class);

        // timestamp with timezone operators and functions
        builder
                .scalar(TimestampWithTimeZoneOperators.Equal.class)
                .scalar(TimestampWithTimeZoneOperators.NotEqual.class)
                .scalar(TimestampWithTimeZoneOperators.LessThan.class)
                .scalar(TimestampWithTimeZoneOperators.LessThanOrEqual.class)
                .scalar(TimestampWithTimeZoneOperators.GreaterThan.class)
                .scalar(TimestampWithTimeZoneOperators.GreaterThanOrEqual.class)
                .scalar(TimestampWithTimeZoneOperators.HashCode.class)
                .scalar(TimestampWithTimeZoneOperators.Indeterminate.class)
                .scalar(TimestampWithTimeZoneOperators.XxHash64Operator.class)
                .scalar(TimestampWithTimeZoneDistinctFromOperator.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampPlusIntervalDayToSecond.class)
                .scalar(TimestampWithTimeZoneOperators.IntervalDayToSecondPlusTimestamp.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusIntervalDayToSecond.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampPlusIntervalYearToMonth.class)
                .scalar(TimestampWithTimeZoneOperators.IntervalYearToMonthPlusTimestamp.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusIntervalYearToMonth.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusTimestamp.class)
                .scalar(CurrentTimestamp.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractYear.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractQuarter.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractMonth.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractDay.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractHour.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractMinute.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractSecond.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractMillisecond.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractDayOfYear.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractDayOfWeek.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractWeekOfYear.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ExtractYearOfWeek.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ToIso8601.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.DateAdd.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.DateTrunc.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.TimeZoneHour.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.TimeZoneMinute.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.DateDiff.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.DateFormat.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.FormatDateTime.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.ToUnixTime.class)
                .scalar(io.prestosql.operator.scalar.timestamptz.LastDayOfMonth.class)
                .scalar(AtTimeZone.class)
                .scalar(AtTimeZoneWithOffset.class)
                .scalar(DateToTimestampWithTimeZoneCast.class)
                .scalar(TimestampWithTimeZoneToDateCast.class)
                .scalar(TimestampWithTimeZoneToTimeCast.class)
                .scalar(TimestampWithTimeZoneToTimestampWithTimeZoneCast.class)
                .scalar(TimestampWithTimeZoneToTimeWithTimezoneCast.class)
                .scalar(TimestampWithTimeZoneToVarcharCast.class)
                .scalar(TimeToTimestampWithTimeZoneCast.class)
                .scalar(TimeWithTimeZoneToTimestampWithTimeZoneCast.class)
                .scalar(VarcharToTimestampWithTimeZoneCast.class);

        // time without time zone functions and operators
        builder.scalar(LocalTimeFunction.class)
                .scalars(TimeOperators.class)
                .scalars(TimeFunctions.class)
                .scalar(TimeToTimeWithTimeZoneCast.class)
                .scalar(TimeOperators.TimeDistinctFromOperator.class);

        // time with timezone operators and functions
        builder
                .scalar(TimeWithTimeZoneOperators.Equal.class)
                .scalar(TimeWithTimeZoneOperators.NotEqual.class)
                .scalar(TimeWithTimeZoneOperators.LessThan.class)
                .scalar(TimeWithTimeZoneOperators.LessThanOrEqual.class)
                .scalar(TimeWithTimeZoneOperators.GreaterThan.class)
                .scalar(TimeWithTimeZoneOperators.GreaterThanOrEqual.class)
                .scalar(TimeWithTimeZoneOperators.HashCode.class)
                .scalar(TimeWithTimeZoneOperators.Indeterminate.class)
                .scalar(TimeWithTimeZoneOperators.XxHash64Operator.class)
                .scalar(TimeWithTimeZoneDistinctFromOperator.class)
                .scalar(TimeWithTimeZoneOperators.TimePlusIntervalDayToSecond.class)
                .scalar(TimeWithTimeZoneOperators.IntervalDayToSecondPlusTime.class)
                .scalar(TimeWithTimeZoneOperators.TimeMinusIntervalDayToSecond.class)
                .scalar(TimeWithTimeZoneOperators.TimeMinusTime.class)
                .scalar(TimeWithTimeZoneToTimeCast.class)
                .scalar(TimeWithTimeZoneToTimeWithTimeZoneCast.class)
                .scalar(TimeWithTimeZoneToVarcharCast.class)
                .scalar(VarcharToTimeWithTimeZoneCast.class)
                .scalar(io.prestosql.operator.scalar.timetz.DateDiff.class)
                .scalar(io.prestosql.operator.scalar.timetz.DateAdd.class)
                .scalar(io.prestosql.operator.scalar.timetz.ExtractHour.class)
                .scalar(io.prestosql.operator.scalar.timetz.ExtractMinute.class)
                .scalar(io.prestosql.operator.scalar.timetz.ExtractSecond.class)
                .scalar(io.prestosql.operator.scalar.timetz.ExtractMillisecond.class)
                .scalar(io.prestosql.operator.scalar.timetz.DateTrunc.class)
                .scalar(io.prestosql.operator.scalar.timetz.AtTimeZone.class)
                .scalar(io.prestosql.operator.scalar.timetz.AtTimeZoneWithOffset.class)
                .scalar(CurrentTime.class);

        switch (featuresConfig.getRegexLibrary()) {
            case JONI:
                builder.scalars(JoniRegexpFunctions.class);
                builder.scalar(JoniRegexpReplaceLambdaFunction.class);
                break;
            case RE2J:
                builder.scalars(Re2JRegexpFunctions.class);
                builder.scalar(Re2JRegexpReplaceLambdaFunction.class);
                break;
        }

        addFunctions(builder.getFunctions());
    }

    public final synchronized void addFunctions(List<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            FunctionMetadata functionMetadata = function.getFunctionMetadata();
            checkArgument(!functionMetadata.getSignature().getName().contains("|"), "Function name cannot contain '|' character: %s", functionMetadata.getSignature());
            for (FunctionMetadata existingFunction : this.functions.list()) {
                checkArgument(!functionMetadata.getFunctionId().equals(existingFunction.getFunctionId()), "Function already registered: %s", functionMetadata.getFunctionId());
                checkArgument(!functionMetadata.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", functionMetadata.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    public List<FunctionMetadata> list()
    {
        return functions.list();
    }

    public Collection<FunctionMetadata> get(QualifiedName name)
    {
        return functions.get(name);
    }

    public FunctionMetadata get(FunctionId functionId)
    {
        return functions.get(functionId).getFunctionMetadata();
    }

    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionBinding functionBinding)
    {
        SqlFunction function = functions.get(functionBinding.getFunctionId());
        checkArgument(function instanceof SqlAggregationFunction, "%s is not an aggregation function", functionBinding.getBoundSignature());

        SqlAggregationFunction aggregationFunction = (SqlAggregationFunction) function;
        return aggregationFunction.getAggregationMetadata(functionBinding);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        SqlFunction function = functions.get(functionBinding.getFunctionId());
        try {
            if (function instanceof SqlAggregationFunction) {
                InternalAggregationFunction aggregationFunction = specializedAggregationCache.get(functionBinding, () -> specializedAggregation(functionBinding, functionDependencies));
                return supplier(function.getFunctionMetadata().getSignature(), aggregationFunction);
            }
            return specializedWindowCache.get(functionBinding, () -> specializeWindow(functionBinding, functionDependencies));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private WindowFunctionSupplier specializeWindow(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        SqlWindowFunction function = (SqlWindowFunction) functions.get(functionBinding.getFunctionId());
        return function.specialize(functionBinding, functionDependencies);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        try {
            return specializedAggregationCache.get(functionBinding, () -> specializedAggregation(functionBinding, functionDependencies));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private InternalAggregationFunction specializedAggregation(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        SqlAggregationFunction function = (SqlAggregationFunction) functions.get(functionBinding.getFunctionId());
        return function.specialize(functionBinding, functionDependencies);
    }

    public FunctionDependencyDeclaration getFunctionDependencies(FunctionBinding functionBinding)
    {
        SqlFunction function = functions.get(functionBinding.getFunctionId());
        return function.getFunctionDependencies(functionBinding);
    }

    public FunctionInvoker getScalarFunctionInvoker(
            FunctionBinding functionBinding,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        ScalarFunctionImplementation scalarFunctionImplementation;
        try {
            scalarFunctionImplementation = specializedScalarCache.get(functionBinding, () -> specializeScalarFunction(functionBinding, functionDependencies));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new RuntimeException(e.getCause());
        }
        FunctionInvokerProvider functionInvokerProvider = new FunctionInvokerProvider();
        return functionInvokerProvider.createFunctionInvoker(scalarFunctionImplementation, functionBinding.getBoundSignature(), invocationConvention);
    }

    private ScalarFunctionImplementation specializeScalarFunction(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        SqlScalarFunction function = (SqlScalarFunction) functions.get(functionBinding.getFunctionId());
        return function.specialize(functionBinding, functionDependencies);
    }

    private static class FunctionMap
    {
        private final Map<FunctionId, SqlFunction> functions;
        private final Multimap<QualifiedName, FunctionMetadata> functionsByName;

        public FunctionMap()
        {
            functions = ImmutableMap.of();
            functionsByName = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Collection<? extends SqlFunction> functions)
        {
            this.functions = ImmutableMap.<FunctionId, SqlFunction>builder()
                    .putAll(map.functions)
                    .putAll(Maps.uniqueIndex(functions, function -> function.getFunctionMetadata().getFunctionId()))
                    .build();

            ImmutableListMultimap.Builder<QualifiedName, FunctionMetadata> functionsByName = ImmutableListMultimap.<QualifiedName, FunctionMetadata>builder()
                    .putAll(map.functionsByName);
            functions.stream()
                    .map(SqlFunction::getFunctionMetadata)
                    .forEach(functionMetadata -> functionsByName.put(QualifiedName.of(functionMetadata.getSignature().getName()), functionMetadata));
            this.functionsByName = functionsByName.build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedName, Collection<FunctionMetadata>> entry : this.functionsByName.asMap().entrySet()) {
                Collection<FunctionMetadata> values = entry.getValue();
                long aggregations = values.stream()
                        .map(FunctionMetadata::getKind)
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<FunctionMetadata> list()
        {
            return ImmutableList.copyOf(functionsByName.values());
        }

        public Collection<FunctionMetadata> get(QualifiedName name)
        {
            return functionsByName.get(name);
        }

        public SqlFunction get(FunctionId functionId)
        {
            SqlFunction sqlFunction = functions.get(functionId);
            checkArgument(sqlFunction != null, "Unknown function implementation: " + functionId);
            return sqlFunction;
        }
    }
}
