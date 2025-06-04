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
package io.trino.metadata;

import io.trino.FeaturesConfig;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalFunctionBundle.InternalFunctionBundleBuilder;
import io.trino.operator.aggregation.ApproximateCountDistinctAggregation;
import io.trino.operator.aggregation.ApproximateDoublePercentileAggregations;
import io.trino.operator.aggregation.ApproximateDoublePercentileArrayAggregations;
import io.trino.operator.aggregation.ApproximateLongPercentileAggregations;
import io.trino.operator.aggregation.ApproximateLongPercentileArrayAggregations;
import io.trino.operator.aggregation.ApproximateRealPercentileAggregations;
import io.trino.operator.aggregation.ApproximateRealPercentileArrayAggregations;
import io.trino.operator.aggregation.ApproximateSetAggregation;
import io.trino.operator.aggregation.ApproximateSetGenericAggregation;
import io.trino.operator.aggregation.ArbitraryAggregationFunction;
import io.trino.operator.aggregation.BigintApproximateMostFrequent;
import io.trino.operator.aggregation.BitwiseAndAggregation;
import io.trino.operator.aggregation.BitwiseOrAggregation;
import io.trino.operator.aggregation.BitwiseXorAggregation;
import io.trino.operator.aggregation.BooleanAndAggregation;
import io.trino.operator.aggregation.BooleanApproximateCountDistinctAggregation;
import io.trino.operator.aggregation.BooleanDefaultApproximateCountDistinctAggregation;
import io.trino.operator.aggregation.BooleanOrAggregation;
import io.trino.operator.aggregation.CentralMomentsAggregation;
import io.trino.operator.aggregation.ChecksumAggregationFunction;
import io.trino.operator.aggregation.CountAggregation;
import io.trino.operator.aggregation.CountColumn;
import io.trino.operator.aggregation.CountIfAggregation;
import io.trino.operator.aggregation.DecimalAverageAggregation;
import io.trino.operator.aggregation.DecimalSumAggregation;
import io.trino.operator.aggregation.DefaultApproximateCountDistinctAggregation;
import io.trino.operator.aggregation.DoubleAverageAggregations;
import io.trino.operator.aggregation.DoubleCorrelationAggregation;
import io.trino.operator.aggregation.DoubleCovarianceAggregation;
import io.trino.operator.aggregation.DoubleHistogramAggregation;
import io.trino.operator.aggregation.DoubleRegressionAggregation;
import io.trino.operator.aggregation.DoubleSumAggregation;
import io.trino.operator.aggregation.GeometricMeanAggregations;
import io.trino.operator.aggregation.IntervalDayToSecondAverageAggregation;
import io.trino.operator.aggregation.IntervalDayToSecondSumAggregation;
import io.trino.operator.aggregation.IntervalYearToMonthAverageAggregation;
import io.trino.operator.aggregation.IntervalYearToMonthSumAggregation;
import io.trino.operator.aggregation.LegacyApproximateDoublePercentileAggregations;
import io.trino.operator.aggregation.LegacyApproximateLongPercentileAggregations;
import io.trino.operator.aggregation.LegacyApproximateRealPercentileAggregations;
import io.trino.operator.aggregation.LongAverageAggregations;
import io.trino.operator.aggregation.LongSumAggregation;
import io.trino.operator.aggregation.MapAggregationFunction;
import io.trino.operator.aggregation.MapUnionAggregation;
import io.trino.operator.aggregation.MaxAggregationFunction;
import io.trino.operator.aggregation.MaxByAggregationFunction;
import io.trino.operator.aggregation.MaxDataSizeForStats;
import io.trino.operator.aggregation.MergeHyperLogLogAggregation;
import io.trino.operator.aggregation.MergeQuantileDigestFunction;
import io.trino.operator.aggregation.MergeTDigestAggregation;
import io.trino.operator.aggregation.MinAggregationFunction;
import io.trino.operator.aggregation.MinByAggregationFunction;
import io.trino.operator.aggregation.QuantileDigestAggregationFunction.BigintQuantileDigestAggregationFunction;
import io.trino.operator.aggregation.QuantileDigestAggregationFunction.DoubleQuantileDigestAggregationFunction;
import io.trino.operator.aggregation.QuantileDigestAggregationFunction.RealQuantileDigestAggregationFunction;
import io.trino.operator.aggregation.RealAverageAggregation;
import io.trino.operator.aggregation.RealCorrelationAggregation;
import io.trino.operator.aggregation.RealCovarianceAggregation;
import io.trino.operator.aggregation.RealGeometricMeanAggregations;
import io.trino.operator.aggregation.RealHistogramAggregation;
import io.trino.operator.aggregation.RealRegressionAggregation;
import io.trino.operator.aggregation.RealSumAggregation;
import io.trino.operator.aggregation.SumDataSizeForStats;
import io.trino.operator.aggregation.TDigestAggregationFunction;
import io.trino.operator.aggregation.VarcharApproximateMostFrequent;
import io.trino.operator.aggregation.VarianceAggregation;
import io.trino.operator.aggregation.arrayagg.ArrayAggregationFunction;
import io.trino.operator.aggregation.histogram.Histogram;
import io.trino.operator.aggregation.listagg.ListaggAggregationFunction;
import io.trino.operator.aggregation.minmaxbyn.MaxByNAggregationFunction;
import io.trino.operator.aggregation.minmaxbyn.MinByNAggregationFunction;
import io.trino.operator.aggregation.minmaxn.MaxNAggregationFunction;
import io.trino.operator.aggregation.minmaxn.MinNAggregationFunction;
import io.trino.operator.aggregation.multimapagg.MultimapAggregationFunction;
import io.trino.operator.scalar.ArrayAllMatchFunction;
import io.trino.operator.scalar.ArrayAnyMatchFunction;
import io.trino.operator.scalar.ArrayCardinalityFunction;
import io.trino.operator.scalar.ArrayCombinationsFunction;
import io.trino.operator.scalar.ArrayContains;
import io.trino.operator.scalar.ArrayContainsSequence;
import io.trino.operator.scalar.ArrayDistinctFunction;
import io.trino.operator.scalar.ArrayElementAtFunction;
import io.trino.operator.scalar.ArrayExceptFunction;
import io.trino.operator.scalar.ArrayFilterFunction;
import io.trino.operator.scalar.ArrayHistogramFunction;
import io.trino.operator.scalar.ArrayIntersectFunction;
import io.trino.operator.scalar.ArrayJoin;
import io.trino.operator.scalar.ArrayMaxFunction;
import io.trino.operator.scalar.ArrayMinFunction;
import io.trino.operator.scalar.ArrayNgramsFunction;
import io.trino.operator.scalar.ArrayNoneMatchFunction;
import io.trino.operator.scalar.ArrayPositionFunction;
import io.trino.operator.scalar.ArrayRemoveFunction;
import io.trino.operator.scalar.ArrayReverseFunction;
import io.trino.operator.scalar.ArrayShuffleFunction;
import io.trino.operator.scalar.ArraySliceFunction;
import io.trino.operator.scalar.ArraySortComparatorFunction;
import io.trino.operator.scalar.ArraySortFunction;
import io.trino.operator.scalar.ArrayToArrayCast;
import io.trino.operator.scalar.ArrayTrimFunction;
import io.trino.operator.scalar.ArrayUnionFunction;
import io.trino.operator.scalar.ArrayVectorFunctions;
import io.trino.operator.scalar.ArraysOverlapFunction;
import io.trino.operator.scalar.BitwiseFunctions;
import io.trino.operator.scalar.CharacterStringCasts;
import io.trino.operator.scalar.ColorFunctions;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.operator.scalar.ConcatWsFunction;
import io.trino.operator.scalar.DataSizeFunctions;
import io.trino.operator.scalar.DateTimeFunctions;
import io.trino.operator.scalar.EmptyMapConstructor;
import io.trino.operator.scalar.FailureFunction;
import io.trino.operator.scalar.FormatNumberFunction;
import io.trino.operator.scalar.GenericComparisonUnorderedFirstOperator;
import io.trino.operator.scalar.GenericComparisonUnorderedLastOperator;
import io.trino.operator.scalar.GenericEqualOperator;
import io.trino.operator.scalar.GenericHashCodeOperator;
import io.trino.operator.scalar.GenericIdenticalOperator;
import io.trino.operator.scalar.GenericIndeterminateOperator;
import io.trino.operator.scalar.GenericLessThanOperator;
import io.trino.operator.scalar.GenericLessThanOrEqualOperator;
import io.trino.operator.scalar.GenericReadValueOperator;
import io.trino.operator.scalar.GenericXxHash64Operator;
import io.trino.operator.scalar.HmacFunctions;
import io.trino.operator.scalar.HyperLogLogFunctions;
import io.trino.operator.scalar.IpAddressFunctions;
import io.trino.operator.scalar.JoniRegexpCasts;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.operator.scalar.JoniRegexpReplaceLambdaFunction;
import io.trino.operator.scalar.JsonFunctions;
import io.trino.operator.scalar.JsonOperators;
import io.trino.operator.scalar.LuhnCheckFunction;
import io.trino.operator.scalar.MapCardinalityFunction;
import io.trino.operator.scalar.MapConcatFunction;
import io.trino.operator.scalar.MapEntriesFunction;
import io.trino.operator.scalar.MapFromEntriesFunction;
import io.trino.operator.scalar.MapKeys;
import io.trino.operator.scalar.MapSubscriptOperator;
import io.trino.operator.scalar.MapToMapCast;
import io.trino.operator.scalar.MapTransformKeysFunction;
import io.trino.operator.scalar.MapValues;
import io.trino.operator.scalar.MathFunctions;
import io.trino.operator.scalar.MultimapFromEntriesFunction;
import io.trino.operator.scalar.QuantileDigestFunctions;
import io.trino.operator.scalar.Re2JRegexpFunctions;
import io.trino.operator.scalar.Re2JRegexpReplaceLambdaFunction;
import io.trino.operator.scalar.RepeatFunction;
import io.trino.operator.scalar.SequenceFunction;
import io.trino.operator.scalar.SessionFunctions;
import io.trino.operator.scalar.SplitToMapFunction;
import io.trino.operator.scalar.SplitToMultimapFunction;
import io.trino.operator.scalar.StringFunctions;
import io.trino.operator.scalar.TDigestFunctions;
import io.trino.operator.scalar.TryFunction;
import io.trino.operator.scalar.TypeOfFunction;
import io.trino.operator.scalar.UrlFunctions;
import io.trino.operator.scalar.VarbinaryFunctions;
import io.trino.operator.scalar.VersionFunction;
import io.trino.operator.scalar.WilsonInterval;
import io.trino.operator.scalar.WordStemFunction;
import io.trino.operator.scalar.json.JsonInputFunctions;
import io.trino.operator.scalar.json.JsonOutputFunctions;
import io.trino.operator.scalar.time.LocalTimeFunction;
import io.trino.operator.scalar.time.TimeFunctions;
import io.trino.operator.scalar.time.TimeOperators;
import io.trino.operator.scalar.time.TimeToTimeWithTimeZoneCast;
import io.trino.operator.scalar.time.TimeToTimestampCast;
import io.trino.operator.scalar.time.TimeToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timestamp.DateAdd;
import io.trino.operator.scalar.timestamp.DateDiff;
import io.trino.operator.scalar.timestamp.DateFormat;
import io.trino.operator.scalar.timestamp.DateToTimestampCast;
import io.trino.operator.scalar.timestamp.DateTrunc;
import io.trino.operator.scalar.timestamp.ExtractDay;
import io.trino.operator.scalar.timestamp.ExtractDayOfWeek;
import io.trino.operator.scalar.timestamp.ExtractDayOfYear;
import io.trino.operator.scalar.timestamp.ExtractHour;
import io.trino.operator.scalar.timestamp.ExtractMillisecond;
import io.trino.operator.scalar.timestamp.ExtractMinute;
import io.trino.operator.scalar.timestamp.ExtractMonth;
import io.trino.operator.scalar.timestamp.ExtractQuarter;
import io.trino.operator.scalar.timestamp.ExtractSecond;
import io.trino.operator.scalar.timestamp.ExtractWeekOfYear;
import io.trino.operator.scalar.timestamp.ExtractYear;
import io.trino.operator.scalar.timestamp.ExtractYearOfWeek;
import io.trino.operator.scalar.timestamp.FormatDateTime;
import io.trino.operator.scalar.timestamp.HumanReadableSeconds;
import io.trino.operator.scalar.timestamp.LastDayOfMonth;
import io.trino.operator.scalar.timestamp.LocalTimestamp;
import io.trino.operator.scalar.timestamp.SequenceIntervalDayToSecond;
import io.trino.operator.scalar.timestamp.SequenceIntervalYearToMonth;
import io.trino.operator.scalar.timestamp.TimeWithTimeZoneToTimestampCast;
import io.trino.operator.scalar.timestamp.TimestampOperators;
import io.trino.operator.scalar.timestamp.TimestampToDateCast;
import io.trino.operator.scalar.timestamp.TimestampToJsonCast;
import io.trino.operator.scalar.timestamp.TimestampToTimeCast;
import io.trino.operator.scalar.timestamp.TimestampToTimeWithTimeZoneCast;
import io.trino.operator.scalar.timestamp.TimestampToTimestampCast;
import io.trino.operator.scalar.timestamp.TimestampToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timestamp.TimestampToVarcharCast;
import io.trino.operator.scalar.timestamp.ToIso8601;
import io.trino.operator.scalar.timestamp.VarcharToTimestampCast;
import io.trino.operator.scalar.timestamp.WithTimeZone;
import io.trino.operator.scalar.timestamptz.AtTimeZone;
import io.trino.operator.scalar.timestamptz.AtTimeZoneWithOffset;
import io.trino.operator.scalar.timestamptz.CurrentTimestamp;
import io.trino.operator.scalar.timestamptz.DateToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneOperators;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToDateCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToTimeCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToTimeWithTimeZoneCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToTimestampCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToVarcharCast;
import io.trino.operator.scalar.timestamptz.VarcharToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timetz.CurrentTime;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneOperators;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneToTimeCast;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneToTimeWithTimeZoneCast;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneToVarcharCast;
import io.trino.operator.scalar.timetz.VarcharToTimeWithTimeZoneCast;
import io.trino.operator.window.CumulativeDistributionFunction;
import io.trino.operator.window.DenseRankFunction;
import io.trino.operator.window.FirstValueFunction;
import io.trino.operator.window.LagFunction;
import io.trino.operator.window.LastValueFunction;
import io.trino.operator.window.LeadFunction;
import io.trino.operator.window.NTileFunction;
import io.trino.operator.window.NthValueFunction;
import io.trino.operator.window.PercentRankFunction;
import io.trino.operator.window.RankFunction;
import io.trino.operator.window.RowNumberFunction;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.type.BigintOperators;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BooleanOperators;
import io.trino.type.DateOperators;
import io.trino.type.DateTimeOperators;
import io.trino.type.DecimalOperators;
import io.trino.type.DoubleOperators;
import io.trino.type.HyperLogLogOperators;
import io.trino.type.IntegerOperators;
import io.trino.type.IntervalDayTimeOperators;
import io.trino.type.IntervalYearMonthOperators;
import io.trino.type.IpAddressOperators;
import io.trino.type.LikeFunctions;
import io.trino.type.QuantileDigestOperators;
import io.trino.type.RealOperators;
import io.trino.type.SmallintOperators;
import io.trino.type.TDigestOperators;
import io.trino.type.TinyintOperators;
import io.trino.type.UuidOperators;
import io.trino.type.VarcharOperators;
import io.trino.type.setdigest.BuildSetDigestAggregation;
import io.trino.type.setdigest.MergeSetDigestAggregation;
import io.trino.type.setdigest.SetDigestFunctions;
import io.trino.type.setdigest.SetDigestOperators;

import static io.trino.operator.aggregation.ReduceAggregationFunction.REDUCE_AGG;
import static io.trino.operator.scalar.ArrayConcatFunction.ARRAY_CONCAT_FUNCTION;
import static io.trino.operator.scalar.ArrayFlattenFunction.ARRAY_FLATTEN_FUNCTION;
import static io.trino.operator.scalar.ArrayReduceFunction.ARRAY_REDUCE_FUNCTION;
import static io.trino.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static io.trino.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static io.trino.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_FUNCTION;
import static io.trino.operator.scalar.CastFromUnknownOperator.CAST_FROM_UNKNOWN;
import static io.trino.operator.scalar.ConcatFunction.VARBINARY_CONCAT;
import static io.trino.operator.scalar.ConcatFunction.VARCHAR_CONCAT;
import static io.trino.operator.scalar.ConcatWsFunction.CONCAT_WS;
import static io.trino.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static io.trino.operator.scalar.FormatFunction.FORMAT_FUNCTION;
import static io.trino.operator.scalar.Greatest.GREATEST;
import static io.trino.operator.scalar.IdentityCast.IDENTITY_CAST;
import static io.trino.operator.scalar.JsonStringArrayExtractScalar.JSON_STRING_ARRAY_EXTRACT_SCALAR;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW;
import static io.trino.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static io.trino.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static io.trino.operator.scalar.JsonToRowCast.JSON_TO_ROW;
import static io.trino.operator.scalar.Least.LEAST;
import static io.trino.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static io.trino.operator.scalar.MapElementAtFunction.MAP_ELEMENT_AT;
import static io.trino.operator.scalar.MapFilterFunction.MAP_FILTER_FUNCTION;
import static io.trino.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static io.trino.operator.scalar.MapTransformValuesFunction.MAP_TRANSFORM_VALUES_FUNCTION;
import static io.trino.operator.scalar.MapZipWithFunction.MAP_ZIP_WITH_FUNCTION;
import static io.trino.operator.scalar.MathFunctions.DECIMAL_MOD_FUNCTION;
import static io.trino.operator.scalar.Re2JCastToRegexpFunction.castCharToRe2JRegexp;
import static io.trino.operator.scalar.Re2JCastToRegexpFunction.castVarcharToRe2JRegexp;
import static io.trino.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static io.trino.operator.scalar.RowToRowCast.ROW_TO_ROW_CAST;
import static io.trino.operator.scalar.TryCastFunction.TRY_CAST;
import static io.trino.operator.scalar.ZipFunction.ZIP_FUNCTIONS;
import static io.trino.operator.scalar.ZipWithFunction.ZIP_WITH_FUNCTION;
import static io.trino.operator.scalar.json.JsonArrayFunction.JSON_ARRAY_FUNCTION;
import static io.trino.operator.scalar.json.JsonObjectFunction.JSON_OBJECT_FUNCTION;
import static io.trino.type.DecimalCasts.BIGINT_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.BOOLEAN_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_BIGINT_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_BOOLEAN_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_DOUBLE_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_INTEGER_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_JSON_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_REAL_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_SMALLINT_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_TINYINT_CAST;
import static io.trino.type.DecimalCasts.DECIMAL_TO_VARCHAR_CAST;
import static io.trino.type.DecimalCasts.DOUBLE_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.INTEGER_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.JSON_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.REAL_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.SMALLINT_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.TINYINT_TO_DECIMAL_CAST;
import static io.trino.type.DecimalCasts.VARCHAR_TO_DECIMAL_CAST;
import static io.trino.type.DecimalOperators.DECIMAL_ADD_OPERATOR;
import static io.trino.type.DecimalOperators.DECIMAL_DIVIDE_OPERATOR;
import static io.trino.type.DecimalOperators.DECIMAL_MODULUS_OPERATOR;
import static io.trino.type.DecimalOperators.DECIMAL_MULTIPLY_OPERATOR;
import static io.trino.type.DecimalOperators.DECIMAL_SUBTRACT_OPERATOR;
import static io.trino.type.DecimalSaturatedFloorCasts.BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalSaturatedFloorCasts.TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static io.trino.type.DecimalToDecimalCasts.DECIMAL_TO_DECIMAL_CAST;

public final class SystemFunctionBundle
{
    private SystemFunctionBundle() {}

    public static FunctionBundle create(FeaturesConfig featuresConfig, TypeOperators typeOperators, BlockTypeOperators blockTypeOperators, NodeVersion nodeVersion)
    {
        InternalFunctionBundleBuilder builder = InternalFunctionBundle.builder()
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
                .aggregates(ApproximateCountDistinctAggregation.class)
                .aggregates(DefaultApproximateCountDistinctAggregation.class)
                .aggregates(BooleanApproximateCountDistinctAggregation.class)
                .aggregates(BooleanDefaultApproximateCountDistinctAggregation.class)
                .aggregates(SumDataSizeForStats.class)
                .aggregates(MaxDataSizeForStats.class)
                .aggregates(CountAggregation.class)
                .aggregates(VarianceAggregation.class)
                .aggregates(CentralMomentsAggregation.class)
                .aggregates(ApproximateLongPercentileAggregations.class)
                .aggregates(LegacyApproximateLongPercentileAggregations.class)
                .aggregates(ApproximateLongPercentileArrayAggregations.class)
                .aggregates(ApproximateDoublePercentileAggregations.class)
                .aggregates(LegacyApproximateDoublePercentileAggregations.class)
                .aggregates(ApproximateDoublePercentileArrayAggregations.class)
                .aggregates(ApproximateRealPercentileAggregations.class)
                .aggregates(LegacyApproximateRealPercentileAggregations.class)
                .aggregates(ApproximateRealPercentileArrayAggregations.class)
                .aggregates(CountIfAggregation.class)
                .aggregates(BooleanAndAggregation.class)
                .aggregates(BooleanOrAggregation.class)
                .aggregates(DoubleSumAggregation.class)
                .aggregates(RealSumAggregation.class)
                .aggregates(LongSumAggregation.class)
                .aggregates(IntervalDayToSecondSumAggregation.class)
                .aggregates(IntervalYearToMonthSumAggregation.class)
                .aggregates(LongAverageAggregations.class)
                .aggregates(DoubleAverageAggregations.class)
                .aggregates(RealAverageAggregation.class)
                .aggregates(IntervalDayToSecondAverageAggregation.class)
                .aggregates(IntervalYearToMonthAverageAggregation.class)
                .aggregates(GeometricMeanAggregations.class)
                .aggregates(RealGeometricMeanAggregations.class)
                .aggregates(MergeHyperLogLogAggregation.class)
                .aggregates(ApproximateSetAggregation.class)
                .aggregates(ApproximateSetGenericAggregation.class)
                .aggregates(TDigestAggregationFunction.class)
                .aggregates(DoubleQuantileDigestAggregationFunction.class)
                .aggregates(RealQuantileDigestAggregationFunction.class)
                .aggregates(BigintQuantileDigestAggregationFunction.class)
                .aggregates(MergeQuantileDigestFunction.class)
                .aggregates(MergeTDigestAggregation.class)
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
                .aggregates(BitwiseXorAggregation.class)
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
                .scalar(DateTimeFunctions.FromUnixtimeNanosDecimal.class)
                .scalars(JsonFunctions.class)
                .scalars(JsonInputFunctions.class)
                .scalars(JsonOutputFunctions.class)
                .functions(JSON_OBJECT_FUNCTION, JSON_ARRAY_FUNCTION)
                .scalars(ColorFunctions.class)
                .scalars(HyperLogLogFunctions.class)
                .scalars(QuantileDigestFunctions.class)
                .scalars(TDigestFunctions.class)
                .scalars(BooleanOperators.class)
                .scalars(BigintOperators.class)
                .scalars(IntegerOperators.class)
                .scalars(SmallintOperators.class)
                .scalars(TinyintOperators.class)
                .scalars(DoubleOperators.class)
                .scalars(RealOperators.class)
                .scalars(VarcharOperators.class)
                .scalars(DateOperators.class)
                .scalars(IntervalDayTimeOperators.class)
                .scalars(IntervalYearMonthOperators.class)
                .scalars(DateTimeOperators.class)
                .scalars(HyperLogLogOperators.class)
                .scalars(QuantileDigestOperators.class)
                .scalars(TDigestOperators.class)
                .scalars(IpAddressOperators.class)
                .scalars(IpAddressFunctions.class)
                .scalars(UuidOperators.class)
                .scalars(LikeFunctions.class)
                .scalars(HmacFunctions.class)
                .scalars(DataSizeFunctions.class)
                .scalars(FormatNumberFunction.class)
                .scalar(ArrayCardinalityFunction.class)
                .scalar(ArrayContains.class)
                .scalar(ArrayContainsSequence.class)
                .scalar(ArrayFilterFunction.class)
                .scalar(ArrayPositionFunction.class)
                .scalars(ArrayVectorFunctions.class)
                .scalars(CombineHashFunction.class)
                .scalars(JsonOperators.class)
                .scalars(FailureFunction.class)
                .scalars(JoniRegexpCasts.class)
                .scalars(CharacterStringCasts.class)
                .scalars(LuhnCheckFunction.class)
                .scalar(DecimalOperators.Negation.class)
                .functions(IDENTITY_CAST, CAST_FROM_UNKNOWN)
                .scalar(ArrayRemoveFunction.class)
                .scalar(ArrayElementAtFunction.class)
                .scalar(ArraySortFunction.class)
                .scalar(ArraySortComparatorFunction.class)
                .scalar(ArrayShuffleFunction.class)
                .scalar(ArrayReverseFunction.class)
                .scalar(ArrayMinFunction.class)
                .scalar(ArrayMaxFunction.class)
                .scalar(ArrayDistinctFunction.class)
                .scalar(ArrayIntersectFunction.class)
                .scalar(ArraysOverlapFunction.class)
                .scalar(ArrayUnionFunction.class)
                .scalar(ArrayExceptFunction.class)
                .scalar(ArraySliceFunction.class)
                .scalar(ArrayTrimFunction.class)
                .scalar(ArrayCombinationsFunction.class)
                .scalar(ArrayNgramsFunction.class)
                .scalar(ArrayAllMatchFunction.class)
                .scalar(ArrayAnyMatchFunction.class)
                .scalar(ArrayNoneMatchFunction.class)
                .scalar(MapEntriesFunction.class)
                .scalar(MapFromEntriesFunction.class)
                .scalar(MultimapFromEntriesFunction.class)
                .scalar(MapKeys.class)
                .scalar(MapValues.class)
                .scalar(MapCardinalityFunction.class)
                .scalar(EmptyMapConstructor.class)
                .scalar(TypeOfFunction.class)
                .scalar(TryFunction.class)
                .scalar(ConcatWsFunction.ConcatArrayWs.class)
                .scalar(DynamicFilters.Function.class)
                .scalar(DynamicFilters.NullableFunction.class)
                .functions(ZIP_WITH_FUNCTION, MAP_ZIP_WITH_FUNCTION)
                .functions(ZIP_FUNCTIONS)
                .scalars(ArrayJoin.class)
                .scalar(ArrayToArrayCast.class)
                .functions(ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .function(MAP_ELEMENT_AT)
                .function(new MapConcatFunction(blockTypeOperators))
                .function(new MapToMapCast(blockTypeOperators))
                .function(ARRAY_FLATTEN_FUNCTION)
                .function(ARRAY_CONCAT_FUNCTION)
                .functions(ARRAY_SUBSCRIPT, JSON_TO_ARRAY, JSON_STRING_TO_ARRAY, JSON_STRING_ARRAY_EXTRACT_SCALAR)
                .aggregates(ArrayAggregationFunction.class)
                .aggregates(ListaggAggregationFunction.class)
                .functions(new MapSubscriptOperator())
                .functions(MAP_CONSTRUCTOR, JSON_TO_MAP, JSON_STRING_TO_MAP)
                .aggregates(MapAggregationFunction.class)
                .aggregates(MapUnionAggregation.class)
                .function(REDUCE_AGG)
                .aggregates(MultimapAggregationFunction.class)
                .functions(DECIMAL_TO_VARCHAR_CAST, DECIMAL_TO_INTEGER_CAST, DECIMAL_TO_BIGINT_CAST, DECIMAL_TO_DOUBLE_CAST, DECIMAL_TO_REAL_CAST, DECIMAL_TO_BOOLEAN_CAST, DECIMAL_TO_TINYINT_CAST, DECIMAL_TO_SMALLINT_CAST)
                .functions(VARCHAR_TO_DECIMAL_CAST, INTEGER_TO_DECIMAL_CAST, BIGINT_TO_DECIMAL_CAST, DOUBLE_TO_DECIMAL_CAST, REAL_TO_DECIMAL_CAST, BOOLEAN_TO_DECIMAL_CAST, TINYINT_TO_DECIMAL_CAST, SMALLINT_TO_DECIMAL_CAST)
                .functions(JSON_TO_DECIMAL_CAST, DECIMAL_TO_JSON_CAST)
                .functions(DECIMAL_ADD_OPERATOR, DECIMAL_SUBTRACT_OPERATOR, DECIMAL_MULTIPLY_OPERATOR, DECIMAL_DIVIDE_OPERATOR, DECIMAL_MODULUS_OPERATOR)
                .function(DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST, BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST, INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST, SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST, TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .aggregates(Histogram.class)
                .aggregates(ChecksumAggregationFunction.class)
                .aggregates(ArbitraryAggregationFunction.class)
                .functions(GREATEST, LEAST)
                .aggregates(MinAggregationFunction.class)
                .aggregates(MaxAggregationFunction.class)
                .aggregates(MinByAggregationFunction.class)
                .aggregates(MaxByAggregationFunction.class)
                .aggregates(MaxNAggregationFunction.class)
                .aggregates(MinNAggregationFunction.class)
                .aggregates(MinByNAggregationFunction.class)
                .aggregates(MaxByNAggregationFunction.class)
                .aggregates(CountColumn.class)
                .functions(JSON_TO_ROW, JSON_STRING_TO_ROW, ROW_TO_ROW_CAST)
                .functions(ROW_TO_JSON, ARRAY_TO_JSON, MAP_TO_JSON)
                .functions(VARCHAR_CONCAT, VARBINARY_CONCAT)
                .function(CONCAT_WS)
                .function(DECIMAL_TO_DECIMAL_CAST)
                .function(castVarcharToRe2JRegexp(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()))
                .function(castCharToRe2JRegexp(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()))
                .aggregates(DecimalAverageAggregation.class)
                .aggregates(DecimalSumAggregation.class)
                .function(DECIMAL_MOD_FUNCTION)
                .functions(ARRAY_TRANSFORM_FUNCTION, ARRAY_REDUCE_FUNCTION)
                .functions(MAP_FILTER_FUNCTION, new MapTransformKeysFunction(blockTypeOperators), MAP_TRANSFORM_VALUES_FUNCTION)
                .function(FORMAT_FUNCTION)
                .function(TRY_CAST)
                .function(new GenericReadValueOperator(typeOperators))
                .function(new GenericEqualOperator(typeOperators))
                .function(new GenericHashCodeOperator(typeOperators))
                .function(new GenericXxHash64Operator(typeOperators))
                .function(new GenericIdenticalOperator(typeOperators))
                .function(new GenericIndeterminateOperator(typeOperators))
                .function(new GenericComparisonUnorderedLastOperator(typeOperators))
                .function(new GenericComparisonUnorderedFirstOperator(typeOperators))
                .function(new GenericLessThanOperator(typeOperators))
                .function(new GenericLessThanOrEqualOperator(typeOperators))
                .function(new VersionFunction(nodeVersion.getVersion()))
                .aggregates(MergeSetDigestAggregation.class)
                .aggregates(BuildSetDigestAggregation.class)
                .scalars(SetDigestFunctions.class)
                .scalars(SetDigestOperators.class)
                .scalars(WilsonInterval.class)
                .aggregates(BigintApproximateMostFrequent.class)
                .aggregates(VarcharApproximateMostFrequent.class)
                .scalar(ArrayHistogramFunction.class);

        // timestamp operators and functions
        builder
                .scalar(TimestampOperators.TimestampPlusIntervalDayToSecond.class)
                .scalar(TimestampOperators.IntervalDayToSecondPlusTimestamp.class)
                .scalar(TimestampOperators.TimestampPlusIntervalYearToMonth.class)
                .scalar(TimestampOperators.IntervalYearToMonthPlusTimestamp.class)
                .scalar(TimestampOperators.TimestampMinusIntervalDayToSecond.class)
                .scalar(TimestampOperators.TimestampMinusIntervalYearToMonth.class)
                .scalar(TimestampOperators.TimestampMinusTimestamp.class)
                .scalar(TimestampToTimestampCast.class)
                .scalar(TimestampToTimeCast.class)
                .scalar(TimestampToTimeWithTimeZoneCast.class)
                .scalar(TimestampToTimestampWithTimeZoneCast.class)
                .scalar(TimestampToDateCast.class)
                .scalar(TimestampToVarcharCast.class)
                .scalar(TimestampToJsonCast.class)
                .scalar(DateToTimestampCast.class)
                .scalar(TimeToTimestampCast.class)
                .scalar(TimeWithTimeZoneToTimestampCast.class)
                .scalar(TimestampWithTimeZoneToTimestampCast.class)
                .scalar(VarcharToTimestampCast.class)
                .scalar(LocalTimestamp.class)
                .scalar(DateTrunc.class)
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
                .scalar(TimestampWithTimeZoneOperators.TimestampPlusIntervalDayToSecond.class)
                .scalar(TimestampWithTimeZoneOperators.IntervalDayToSecondPlusTimestamp.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusIntervalDayToSecond.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampPlusIntervalYearToMonth.class)
                .scalar(TimestampWithTimeZoneOperators.IntervalYearToMonthPlusTimestamp.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusIntervalYearToMonth.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampMinusTimestamp.class)
                .scalar(CurrentTimestamp.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractYear.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractQuarter.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractMonth.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractDay.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractHour.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractMinute.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractSecond.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractMillisecond.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractDayOfYear.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractDayOfWeek.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractWeekOfYear.class)
                .scalar(io.trino.operator.scalar.timestamptz.ExtractYearOfWeek.class)
                .scalar(io.trino.operator.scalar.timestamptz.ToIso8601.class)
                .scalar(io.trino.operator.scalar.timestamptz.DateAdd.class)
                .scalar(io.trino.operator.scalar.timestamptz.DateTrunc.class)
                .scalar(io.trino.operator.scalar.timestamptz.TimeZone.class)
                .scalar(io.trino.operator.scalar.timestamptz.TimeZoneHour.class)
                .scalar(io.trino.operator.scalar.timestamptz.TimeZoneMinute.class)
                .scalar(io.trino.operator.scalar.timestamptz.DateDiff.class)
                .scalar(io.trino.operator.scalar.timestamptz.DateFormat.class)
                .scalar(io.trino.operator.scalar.timestamptz.FormatDateTime.class)
                .scalar(io.trino.operator.scalar.timestamptz.ToUnixTime.class)
                .scalar(io.trino.operator.scalar.timestamptz.LastDayOfMonth.class)
                .scalar(AtTimeZone.class)
                .scalar(AtTimeZoneWithOffset.class)
                .scalar(DateToTimestampWithTimeZoneCast.class)
                .scalar(TimestampWithTimeZoneToDateCast.class)
                .scalar(TimestampWithTimeZoneToTimeCast.class)
                .scalar(TimestampWithTimeZoneToTimestampWithTimeZoneCast.class)
                .scalar(TimestampWithTimeZoneToTimeWithTimeZoneCast.class)
                .scalar(TimestampWithTimeZoneToVarcharCast.class)
                .scalar(TimeToTimestampWithTimeZoneCast.class)
                .scalar(TimeWithTimeZoneToTimestampWithTimeZoneCast.class)
                .scalar(VarcharToTimestampWithTimeZoneCast.class);

        // time without time zone functions and operators
        builder.scalar(LocalTimeFunction.class)
                .scalars(TimeOperators.class)
                .scalars(TimeFunctions.class)
                .scalar(TimeToTimeWithTimeZoneCast.class);

        // time with timezone operators and functions
        builder
                .scalar(TimeWithTimeZoneOperators.TimePlusIntervalDayToSecond.class)
                .scalar(TimeWithTimeZoneOperators.IntervalDayToSecondPlusTime.class)
                .scalar(TimeWithTimeZoneOperators.TimeMinusIntervalDayToSecond.class)
                .scalar(TimeWithTimeZoneOperators.TimeMinusTime.class)
                .scalar(TimeWithTimeZoneToTimeCast.class)
                .scalar(TimeWithTimeZoneToTimeWithTimeZoneCast.class)
                .scalar(TimeWithTimeZoneToVarcharCast.class)
                .scalar(VarcharToTimeWithTimeZoneCast.class)
                .scalar(io.trino.operator.scalar.timetz.DateDiff.class)
                .scalar(io.trino.operator.scalar.timetz.DateAdd.class)
                .scalar(io.trino.operator.scalar.timetz.ExtractHour.class)
                .scalar(io.trino.operator.scalar.timetz.ExtractMinute.class)
                .scalar(io.trino.operator.scalar.timetz.ExtractSecond.class)
                .scalar(io.trino.operator.scalar.timetz.ExtractMillisecond.class)
                .scalar(io.trino.operator.scalar.timetz.TimeZone.class)
                .scalar(io.trino.operator.scalar.timetz.TimeZoneHour.class)
                .scalar(io.trino.operator.scalar.timetz.TimeZoneMinute.class)
                .scalar(io.trino.operator.scalar.timetz.DateTrunc.class)
                .scalar(io.trino.operator.scalar.timetz.AtTimeZone.class)
                .scalar(io.trino.operator.scalar.timetz.AtTimeZoneWithOffset.class)
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

        return builder.build();
    }
}
