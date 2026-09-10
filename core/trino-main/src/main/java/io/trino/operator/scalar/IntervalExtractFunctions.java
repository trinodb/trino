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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.PolymorphicScalarFunctionBuilder.SpecializeContext;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.Type;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.LongInterval;

import java.util.List;
import java.util.function.Function;

import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.MINUTE;
import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;

/// The interval field accessors that back `EXTRACT(field FROM interval)`.
///
/// The most significant (leading) field of an interval holds the whole
/// magnitude of the interval in that unit, while every other field is bounded by
/// the next larger field (for example minutes run 0-59). Because the leading
/// field depends on the interval's qualifier, the accessors for fields that can
/// be either leading or trailing read the qualifier to decide which to return.
public final class IntervalExtractFunctions
{
    private static final long MICROS_PER_MILLI = 1000;
    private static final long MICROS_PER_SECOND = 1000 * MICROS_PER_MILLI;
    private static final long MICROS_PER_MINUTE = 60 * MICROS_PER_SECOND;
    private static final long MICROS_PER_HOUR = 60 * MICROS_PER_MINUTE;
    private static final long MICROS_PER_DAY = 24 * MICROS_PER_HOUR;
    private static final long MONTHS_PER_YEAR = 12;
    private static final long LEADING = 0;

    private static final Function<Type, IntervalField> DAY_TIME_START = interval -> ((IntervalDayTimeType) interval).getStartField();
    private static final Function<Type, IntervalField> YEAR_MONTH_START = interval -> ((IntervalYearMonthType) interval).getStartField();

    // YEAR and DAY are always the leading field of their class, so they hold the whole magnitude
    public static final SqlScalarFunction INTERVAL_YEAR = fixedField(
            "year", "Year of the given interval", INTERVAL_YEAR_TO_MONTH, MONTHS_PER_YEAR, LEADING);
    public static final SqlScalarFunction INTERVAL_DAY = fixedField(
            "day", "Day of the given interval", INTERVAL_DAY_TO_SECOND, MICROS_PER_DAY, LEADING);
    // MILLISECOND is always bounded by the second
    public static final SqlScalarFunction INTERVAL_MILLISECOND = fixedField(
            "millisecond", "Millisecond of the second of the given interval", INTERVAL_DAY_TO_SECOND, MICROS_PER_MILLI, MICROS_PER_SECOND);

    public static final SqlScalarFunction INTERVAL_MONTH = qualifierAwareField(
            "month", "Month of the year of the given interval", INTERVAL_YEAR_TO_MONTH, MONTH, 1, MONTHS_PER_YEAR, YEAR_MONTH_START);
    public static final SqlScalarFunction INTERVAL_HOUR = qualifierAwareField(
            "hour", "Hour of the day of the given interval", INTERVAL_DAY_TO_SECOND, HOUR, MICROS_PER_HOUR, MICROS_PER_DAY, DAY_TIME_START);
    public static final SqlScalarFunction INTERVAL_MINUTE = qualifierAwareField(
            "minute", "Minute of the hour of the given interval", INTERVAL_DAY_TO_SECOND, MINUTE, MICROS_PER_MINUTE, MICROS_PER_HOUR, DAY_TIME_START);
    public static final SqlScalarFunction INTERVAL_SECOND = qualifierAwareField(
            "second", "Second of the minute of the given interval", INTERVAL_DAY_TO_SECOND, SECOND, MICROS_PER_SECOND, MICROS_PER_MINUTE, DAY_TIME_START);

    private IntervalExtractFunctions() {}

    private static SqlScalarFunction fixedField(String name, String description, String intervalBaseType, long fieldUnit, long parentUnit)
    {
        return function(name, description, intervalBaseType, _ -> ImmutableList.of(fieldUnit, parentUnit));
    }

    private static SqlScalarFunction qualifierAwareField(
            String name,
            String description,
            String intervalBaseType,
            IntervalField field,
            long fieldUnit,
            long parentUnit,
            Function<Type, IntervalField> startField)
    {
        return function(name, description, intervalBaseType, context -> {
            boolean leading = startField.apply(context.getParameterTypes().get(0)) == field;
            return ImmutableList.of(fieldUnit, leading ? LEADING : parentUnit);
        });
    }

    private static SqlScalarFunction function(String name, String description, String intervalBaseType, Function<SpecializeContext, List<Object>> extraParameters)
    {
        // A day-time interval carries a fourth parameter, the fractional-seconds precision, and its long
        // (picosecond) form reads as a LongInterval; a year-month interval has neither.
        boolean dayTime = intervalBaseType.equals(INTERVAL_DAY_TO_SECOND);
        Signature signature = dayTime
                ? Signature.builder()
                  .argumentType(type(intervalBaseType, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                  .returnType(type(BIGINT))
                  .build()
                : Signature.builder()
                  .argumentType(type(intervalBaseType, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                  .returnType(type(BIGINT))
                  .build();
        String[] methods = dayTime ? new String[] {"extract", "extractLong"} : new String[] {"extract"};
        return new PolymorphicScalarFunctionBuilder(name, IntervalExtractFunctions.class)
                .signature(signature)
                .description(description)
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                        .methods(methods)
                        .withExtraParameters(extraParameters)))
                .build();
    }

    /// Returns the value of an interval field. The interval is given as its whole
    /// magnitude in the field's unit (`total`). A `parentUnit` of `0` denotes the
    /// leading field, whose value is the whole magnitude; otherwise the value is
    /// bounded by the next larger field.
    @UsedByGeneratedCode
    public static long extract(long total, long fieldUnit, long parentUnit)
    {
        long value = (parentUnit == 0) ? total : total % parentUnit;
        return value / fieldUnit;
    }

    /// The long (picosecond) form of a day-time interval; the extracted field never reaches below the
    /// microsecond, so the sub-microsecond fraction is ignored.
    @UsedByGeneratedCode
    public static long extractLong(LongInterval total, long fieldUnit, long parentUnit)
    {
        return extract(total.getMicros(), fieldUnit, parentUnit);
    }
}
