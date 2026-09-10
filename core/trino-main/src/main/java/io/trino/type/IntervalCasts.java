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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Signature;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.IntervalDayTime.formatInterval;
import static io.trino.client.IntervalDayTime.formatMicros;
import static io.trino.client.IntervalYearMonth.formatMonths;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.MINUTE;
import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.IntervalField.YEAR;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.type.DateTimes.roundToNearest;
import static io.trino.type.IntervalDayTimeType.MAX_FRACTIONAL_PRECISION;
import static io.trino.type.IntervalDayTimeType.MAX_SHORT_FRACTIONAL_PRECISION;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseDayTimeIntervalToPicos;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.negateExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/// Casts between interval types, and between an interval and a varchar or an integer.
///
/// A year-month interval is stored as a month count. A day-time interval is stored as microseconds when its
/// fractional-seconds precision is 6 or less, or as a picosecond `LongInterval` for precision 7 to 12, so
/// casting to a different fractional precision may re-encode the value even though changing only the leading
/// fields leaves it unchanged.
public final class IntervalCasts
{
    public static final SqlScalarFunction INTERVAL_DAY_TIME_TO_INTERVAL_DAY_TIME_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("from_start"), numericVariable("from_end"), numericVariable("from_precision"), numericVariable("from_fractional")))
                    .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("to_start"), numericVariable("to_end"), numericVariable("to_precision"), numericVariable("to_fractional")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("dayTimeToDayTimeShortToShort", "dayTimeToDayTimeShortToLong", "dayTimeToDayTimeLongToShort", "dayTimeToDayTimeLongToLong")
                    .withExtraParameters(context -> targetLeadingField(context.getReturnType()))))
            .build();

    public static final SqlScalarFunction INTERVAL_YEAR_MONTH_TO_INTERVAL_YEAR_MONTH_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("from_start"), numericVariable("from_end"), numericVariable("from_precision")))
                    .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("to_start"), numericVariable("to_end"), numericVariable("to_precision")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("yearMonthToYearMonth")
                    .withExtraParameters(context -> targetLeadingField(context.getReturnType()))))
            .build();

    public static final SqlScalarFunction INTERVAL_DAY_TIME_TO_VARCHAR_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                    .returnType(type("varchar", numericVariable("x")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("dayTimeToVarchar", "dayTimeToVarcharLong")
                    .withExtraParameters(context -> ImmutableList.of(varcharLength(context.getReturnType()), (long) ((IntervalDayTimeType) context.getParameterTypes().get(0)).getFractionalPrecision()))))
            .build();

    public static final SqlScalarFunction INTERVAL_YEAR_MONTH_TO_VARCHAR_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                    .returnType(type("varchar", numericVariable("x")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("yearMonthToVarchar")
                    .withExtraParameters(context -> ImmutableList.of(varcharLength(context.getReturnType())))))
            .build();

    public static final SqlScalarFunction VARCHAR_TO_INTERVAL_DAY_TIME_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type("varchar", numericVariable("x")))
                    .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("varcharToDayTime", "varcharToDayTimeLong")
                    .withExtraParameters(context -> {
                        IntervalDayTimeType type = (IntervalDayTimeType) context.getReturnType();
                        return ImmutableList.of(type.getStartField(), type.getEndField(), (long) type.getLeadingPrecision(), (long) type.getFractionalPrecision());
                    })))
            .build();

    public static final SqlScalarFunction VARCHAR_TO_INTERVAL_YEAR_MONTH_CAST = new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
            .signature(Signature.builder()
                    .argumentType(type("varchar", numericVariable("x")))
                    .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                    .methods("varcharToYearMonth")
                    .withExtraParameters(context -> {
                        IntervalYearMonthType type = (IntervalYearMonthType) context.getReturnType();
                        return ImmutableList.of(type.getStartField(), type.getEndField(), (long) type.getLeadingPrecision());
                    })))
            .build();

    // A cast to or from an exact numeric is defined only for an interval with a single field, so the casts are
    // registered against the concrete single-field interval types; a multi-field interval simply has no matching cast.
    public static SqlScalarFunction[] intervalNumericCasts()
    {
        ImmutableList.Builder<SqlScalarFunction> casts = ImmutableList.builder();
        for (IntervalField field : new IntervalField[] {DAY, HOUR, MINUTE, SECOND}) {
            long fieldMicros = dayTimeFieldMicros(field);
            // a single-field SECOND interval may carry a long fractional precision, so its bigint cast needs a LongInterval overload
            casts.add(intervalToBigintCast(INTERVAL_DAY_TO_SECOND, field, fieldMicros, "dayTimeToBigint", "dayTimeToBigintLong"));
            casts.add(numericToIntervalCast(BIGINT, INTERVAL_DAY_TO_SECOND, field, "bigintToDayTime", fieldMicros));
            casts.add(numericToIntervalCast(INTEGER, INTERVAL_DAY_TO_SECOND, field, "bigintToDayTime", fieldMicros));
        }
        for (IntervalField field : new IntervalField[] {YEAR, MONTH}) {
            long fieldMonths = yearMonthFieldMonths(field);
            casts.add(intervalToBigintCast(INTERVAL_YEAR_TO_MONTH, field, fieldMonths, "yearMonthToBigint"));
            casts.add(numericToIntervalCast(BIGINT, INTERVAL_YEAR_TO_MONTH, field, "bigintToYearMonth", fieldMonths));
            casts.add(numericToIntervalCast(INTEGER, INTERVAL_YEAR_TO_MONTH, field, "bigintToYearMonth", fieldMonths));
        }
        return casts.build().toArray(new SqlScalarFunction[0]);
    }

    // The single field is fixed (a literal start == end), but the leading precision is free, so a
    // single-field interval of any width matches while a multi-field interval matches nothing. A
    // day-time interval also carries a fourth parameter, the fractional-seconds precision.
    private static TypeTemplate singleFieldInterval(String base, IntervalField field, NumericExpression precision, NumericExpression fractional)
    {
        NumericExpression fieldCode = new NumericExpression.Literal(field.code());
        if (base.equals(INTERVAL_YEAR_TO_MONTH)) {
            return type(base, fieldCode, fieldCode, precision);
        }
        return type(base, fieldCode, fieldCode, precision, fractional);
    }

    private static int maxLeadingPrecision(IntervalField field)
    {
        return switch (field) {
            case DAY, HOUR, MINUTE, SECOND -> IntervalDayTimeType.maxLeadingPrecision(field);
            case YEAR, MONTH -> IntervalYearMonthType.maxLeadingPrecision(field);
        };
    }

    private static SqlScalarFunction intervalToBigintCast(String base, IntervalField field, long fieldFactor, String... methods)
    {
        return new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
                .signature(Signature.builder()
                        .argumentType(singleFieldInterval(base, field, numericVariable("precision"), numericVariable("fractional")))
                        .returnType(type(BIGINT))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                        .methods(methods)
                        .withExtraParameters(_ -> ImmutableList.of(fieldFactor))))
                .build();
    }

    private static SqlScalarFunction numericToIntervalCast(String numericType, String base, IntervalField field, String method, long fieldFactor)
    {
        return new PolymorphicScalarFunctionBuilder(CAST, IntervalCasts.class)
                .signature(Signature.builder()
                        .argumentType(type(numericType))
                        .returnType(singleFieldInterval(base, field, new NumericExpression.Literal(maxLeadingPrecision(field)), new NumericExpression.Literal(IntervalDayTimeType.defaultFractionalPrecision(field))))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                        .methods(method)
                        .withExtraParameters(_ -> ImmutableList.of(fieldFactor))))
                .build();
    }

    private static long dayTimeFieldMicros(IntervalField field)
    {
        return switch (field) {
            case DAY -> 86_400_000_000L;
            case HOUR -> 3_600_000_000L;
            case MINUTE -> 60_000_000L;
            case SECOND -> 1_000_000L;
            default -> throw new IllegalArgumentException("Not a day-time field: " + field);
        };
    }

    private static long yearMonthFieldMonths(IntervalField field)
    {
        return switch (field) {
            case YEAR -> 12L;
            case MONTH -> 1L;
            default -> throw new IllegalArgumentException("Not a year-month field: " + field);
        };
    }

    private IntervalCasts() {}

    private static long varcharLength(io.trino.spi.type.Type varcharType)
    {
        VarcharType type = (VarcharType) varcharType;
        return type.isUnbounded() ? UNBOUNDED_LENGTH : type.getBoundedLength();
    }

    // The field unit, precision, and leading field of the cast's target type, passed to the interval-to-interval casts
    // so they can reject a value that does not fit the target's leading-field precision.
    private static List<Object> targetLeadingField(io.trino.spi.type.Type target)
    {
        if (target instanceof IntervalDayTimeType type) {
            return ImmutableList.of(dayTimeFieldMicros(type.getStartField()), (long) type.getLeadingPrecision(), type.getStartField(), (long) type.getFractionalPrecision());
        }
        IntervalYearMonthType type = (IntervalYearMonthType) target;
        return ImmutableList.of(yearMonthFieldMonths(type.getStartField()), (long) type.getLeadingPrecision(), type.getStartField());
    }

    @UsedByGeneratedCode
    public static long dayTimeToDayTimeShortToShort(long micros, long fieldMicros, long precision, IntervalField startField, long fractionalPrecision)
    {
        // narrow the value to the target's fractional-seconds precision, then enforce the leading precision
        long rounded = round(micros, MAX_SHORT_FRACTIONAL_PRECISION - (int) fractionalPrecision);
        checkLeadingFieldPrecision(rounded, 0, fieldMicros, precision, startField);
        return rounded;
    }

    @UsedByGeneratedCode
    public static LongInterval dayTimeToDayTimeShortToLong(long micros, long fieldMicros, long precision, IntervalField startField, long fractionalPrecision)
    {
        // a short value has no sub-microsecond fraction, so widening to a long precision only adds zero picoseconds
        checkLeadingFieldPrecision(micros, 0, fieldMicros, precision, startField);
        return new LongInterval(micros, 0);
    }

    @UsedByGeneratedCode
    public static long dayTimeToDayTimeLongToShort(LongInterval value, long fieldMicros, long precision, IntervalField startField, long fractionalPrecision)
    {
        long micros = value.getMicros();
        long rounded;
        if ((int) fractionalPrecision < MAX_SHORT_FRACTIONAL_PRECISION) {
            // rounding below microsecond resolution drops the sub-microsecond fraction without affecting the result
            rounded = round(micros, MAX_SHORT_FRACTIONAL_PRECISION - (int) fractionalPrecision);
        }
        else {
            // microsecond target: the sub-microsecond fraction rounds the microseconds half-up
            rounded = micros;
            if (roundToNearest(value.getPicosOfMicro(), PICOSECONDS_PER_MICROSECOND) == PICOSECONDS_PER_MICROSECOND) {
                rounded = incrementExactMicros(rounded);
            }
        }
        checkLeadingFieldPrecision(rounded, 0, fieldMicros, precision, startField);
        return rounded;
    }

    @UsedByGeneratedCode
    public static LongInterval dayTimeToDayTimeLongToLong(LongInterval value, long fieldMicros, long precision, IntervalField startField, long fractionalPrecision)
    {
        LongInterval rounded = roundToLongInterval(value.getMicros(), value.getPicosOfMicro(), (int) fractionalPrecision);
        checkLeadingFieldPrecision(rounded.getMicros(), rounded.getPicosOfMicro(), fieldMicros, precision, startField);
        return rounded;
    }

    @UsedByGeneratedCode
    public static long yearMonthToYearMonth(long months, long fieldMonths, long precision, IntervalField startField)
    {
        checkLeadingFieldPrecision(months, 0, fieldMonths, precision, startField);
        return months;
    }

    /// Rejects a value whose leading field has more digits than the interval type's leading precision —
    /// the SQL specification's interval field overflow. Shared by the assignment casts and by interval
    /// literals, so that `INTERVAL '340' DAY(2)` fails exactly as `CAST(... AS INTERVAL DAY(2))` does.
    public static void checkLeadingFieldPrecision(long value, io.trino.spi.type.Type intervalType)
    {
        checkLeadingFieldPrecision(value, 0, intervalType);
    }

    /// Long-form overload: `picosOfMicro` is the positive sub-microsecond increment on `micros`, so the
    /// leading field of a floored negative value is measured toward zero rather than one unit too far.
    public static void checkLeadingFieldPrecision(long micros, long picosOfMicro, io.trino.spi.type.Type intervalType)
    {
        if (intervalType instanceof IntervalDayTimeType type) {
            checkLeadingFieldPrecision(micros, picosOfMicro, dayTimeFieldMicros(type.getStartField()), type.getLeadingPrecision(), type.getStartField());
        }
        else {
            IntervalYearMonthType type = (IntervalYearMonthType) intervalType;
            checkLeadingFieldPrecision(micros, picosOfMicro, yearMonthFieldMonths(type.getStartField()), type.getLeadingPrecision(), type.getStartField());
        }
    }

    private static void checkLeadingFieldPrecision(long micros, long picosOfMicro, long fieldUnit, long precision, IntervalField startField)
    {
        long leadingValue = leadingFieldValue(micros, picosOfMicro, fieldUnit);
        long limit = 1;
        for (long digit = 0; digit < precision; digit++) {
            limit *= 10;
        }
        if (leadingValue <= -limit || leadingValue >= limit) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval field overflow: %s %s does not fit a leading precision of %s", leadingValue, startField.keyword(), precision));
        }
    }

    /// The interval's leading-field value, truncated toward zero. The long form stores a positive
    /// `picosOfMicro` increment on a floored `micros`, so at an exact negative field boundary the micros
    /// alone divide one unit too far from zero; the fraction pulls the leading field back toward zero.
    private static long leadingFieldValue(long micros, long picosOfMicro, long fieldUnit)
    {
        long leadingValue = micros / fieldUnit;
        if (micros < 0 && picosOfMicro > 0 && micros % fieldUnit == 0) {
            leadingValue++;
        }
        return leadingValue;
    }

    /// Rounds a (micros, picosOfMicro) pair to a fractional-seconds precision of 7 to 12, producing the
    /// long form. The picoseconds carry into the microseconds when they round up to a full microsecond.
    public static LongInterval roundToLongInterval(long micros, long picosOfMicro, int fractionalPrecision)
    {
        long roundedPicos = round(picosOfMicro, MAX_FRACTIONAL_PRECISION - fractionalPrecision);
        if (roundedPicos >= PICOSECONDS_PER_MICROSECOND) {
            micros = incrementExactMicros(micros);
            roundedPicos = 0;
        }
        return new LongInterval(micros, (int) roundedPicos);
    }

    /// Negates a (micros, picosOfMicro) pair, where picosOfMicro is a positive increment. With a nonzero
    /// increment the negated micros is `-(micros + 1) == ~micros`, which is representable for every micros
    /// (the two's-complement identity never overflows). Only `(Long.MIN_VALUE, 0)` has no representable
    /// negation.
    public static long[] negatePicos(long micros, long picosOfMicro)
    {
        if (picosOfMicro > 0) {
            return new long[] {~micros, PICOSECONDS_PER_MICROSECOND - picosOfMicro};
        }
        try {
            return new long[] {negateExact(micros), 0};
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval", e);
        }
    }

    /// Adds one to a floored microsecond count that carried up out of its sub-microsecond fraction,
    /// translating the long-range overflow at the field maximum to a domain error.
    private static long incrementExactMicros(long micros)
    {
        try {
            return addExact(micros, 1);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice dayTimeToVarchar(long value, long length, long fractionalPrecision)
    {
        return toVarchar(formatMicros(value, (int) fractionalPrecision), length);
    }

    @UsedByGeneratedCode
    public static Slice dayTimeToVarcharLong(LongInterval value, long length, long fractionalPrecision)
    {
        return toVarchar(formatInterval(value.getMicros(), value.getPicosOfMicro(), (int) fractionalPrecision), length);
    }

    @UsedByGeneratedCode
    public static Slice yearMonthToVarchar(long value, long length)
    {
        return toVarchar(formatMonths((int) value), length);
    }

    @UsedByGeneratedCode
    public static long varcharToDayTime(Slice value, IntervalField startField, IntervalField endField, long precision, long fractionalPrecision)
    {
        long micros = round(parseDayTimeInterval(value.toStringUtf8(), startField, endField), MAX_SHORT_FRACTIONAL_PRECISION - (int) fractionalPrecision);
        checkLeadingFieldPrecision(micros, 0, dayTimeFieldMicros(startField), precision, startField);
        return micros;
    }

    @UsedByGeneratedCode
    public static LongInterval varcharToDayTimeLong(Slice value, IntervalField startField, IntervalField endField, long precision, long fractionalPrecision)
    {
        long[] picos = parseDayTimeIntervalToPicos(value.toStringUtf8(), startField, endField);
        LongInterval interval = roundToLongInterval(picos[0], picos[1], (int) fractionalPrecision);
        checkLeadingFieldPrecision(interval.getMicros(), interval.getPicosOfMicro(), dayTimeFieldMicros(startField), precision, startField);
        return interval;
    }

    @UsedByGeneratedCode
    public static long varcharToYearMonth(Slice value, IntervalField startField, IntervalField endField, long precision)
    {
        long months = parseYearMonthInterval(value.toStringUtf8(), startField, endField);
        checkLeadingFieldPrecision(months, 0, yearMonthFieldMonths(startField), precision, startField);
        return months;
    }

    @UsedByGeneratedCode
    public static long dayTimeToBigint(long value, long fieldMicros)
    {
        return value / fieldMicros;
    }

    @UsedByGeneratedCode
    public static long dayTimeToBigintLong(LongInterval value, long fieldMicros)
    {
        // truncate toward zero, including the sub-microsecond fraction so a floored negative value at an
        // exact field boundary matches the short form (e.g. -0.999999999999 SECOND casts to 0, not -1)
        return leadingFieldValue(value.getMicros(), value.getPicosOfMicro(), fieldMicros);
    }

    @UsedByGeneratedCode
    public static long bigintToDayTime(long value, long fieldMicros)
    {
        try {
            return multiplyExact(value, fieldMicros);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval: " + value, e);
        }
    }

    @UsedByGeneratedCode
    public static long yearMonthToBigint(long value, long fieldMonths)
    {
        return value / fieldMonths;
    }

    @UsedByGeneratedCode
    public static long bigintToYearMonth(long value, long fieldMonths)
    {
        try {
            return toIntExact(multiplyExact(value, fieldMonths));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval: " + value, e);
        }
    }

    private static Slice toVarchar(String formatted, long length)
    {
        Slice slice = utf8Slice(formatted);
        // slice is all-ASCII, so slice.length() returns the actual code point count
        if (length == UNBOUNDED_LENGTH || slice.length() <= length) {
            return slice;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", slice.toStringUtf8(), length));
    }
}
