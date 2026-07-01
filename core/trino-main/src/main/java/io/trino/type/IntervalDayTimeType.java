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

import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.TypeDescriptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.MINUTE;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static java.lang.String.format;

/// A day-time interval, given by a qualifier built from [IntervalField#DAY], [IntervalField#HOUR], [IntervalField#MINUTE], and [IntervalField#SECOND]
/// together with the leading field's precision and — when the trailing field is [IntervalField#SECOND] — the
/// fractional-seconds precision. All four are carried in the type's [TypeDescriptor].
///
/// Like `timestamp(p)`, the type has two physical forms: a [short][ShortIntervalDayTimeType] form for
/// a fractional-seconds precision of at most 6 (a signed count of microseconds), and a
/// [long][LongIntervalDayTimeType] form for 7 to 12 (a [LongInterval] of microseconds plus the
/// picoseconds within that microsecond).
public abstract sealed class IntervalDayTimeType
        extends AbstractType
        implements FixedWidthType
        permits ShortIntervalDayTimeType, LongIntervalDayTimeType
{
    public static final String NAME = INTERVAL_DAY_TO_SECOND;

    private static final Map<Long, IntervalDayTimeType> TYPES = new ConcurrentHashMap<>();

    /// The widest leading precision the day-time representation admits, regardless of leading field —
    /// the digit count of the maximum storable magnitude (seconds). Constructing a type is bounded only
    /// by this structural limit; the tighter field-specific [#maxLeadingPrecision] is a semantic rule the
    /// analyzer and the assignment casts enforce where the SQL specification requires it. Decoupling the
    /// two lets function resolution construct the transient cross-field qualifiers it explores while
    /// binding (e.g. pairing a candidate's `day` leading field with an argument's `second(13)` precision)
    /// without the type factory rejecting them outright.
    public static final int MAX_LEADING_PRECISION = 13;

    /// The widest fractional-seconds precision, i.e. picoseconds.
    public static final int MAX_FRACTIONAL_PRECISION = 12;

    /// The widest fractional-seconds precision the microsecond-backed short form can store; above it the
    /// type uses the picosecond-backed long form.
    public static final int MAX_SHORT_FRACTIONAL_PRECISION = 6;

    /// The fractional-seconds precision of a bare `SECOND` trailing field, per the SQL specification.
    public static final int DEFAULT_FRACTIONAL_PRECISION = 6;

    public static final IntervalDayTimeType INTERVAL_DAY_TIME = createIntervalDayTimeType(DAY, SECOND);

    private final IntervalField startField;
    private final IntervalField endField;
    private final int leadingPrecision;
    private final int fractionalPrecision;

    /// The largest leading precision a given leading field can hold in the
    /// microsecond representation, i.e. the digit count of its maximum value.
    public static int maxLeadingPrecision(IntervalField field)
    {
        return switch (field) {
            case DAY -> 9;      // 106751991 days
            case HOUR -> 10;    // 2562047788 hours
            case MINUTE -> 12;  // 153722867280 minutes
            case SECOND -> 13;  // 9223372036854 seconds
            default -> throw new IllegalArgumentException("Not a day-time field: " + field);
        };
    }

    /// The fractional-seconds precision a bare qualifier with the given trailing field carries: the
    /// SQL default of 6 when the trailing field is [IntervalField#SECOND], otherwise none.
    public static int defaultFractionalPrecision(IntervalField endField)
    {
        return endField == SECOND ? DEFAULT_FRACTIONAL_PRECISION : 0;
    }

    /// Creates a day-time interval type whose leading precision is the field maximum and whose
    /// fractional-seconds precision is the trailing field's default — the precision sufficient to hold
    /// any value, used for the canonical type and for derived intervals.
    public static IntervalDayTimeType createIntervalDayTimeType(IntervalField startField, IntervalField endField)
    {
        return createIntervalDayTimeType(startField, endField, maxLeadingPrecision(startField));
    }

    /// Creates a day-time interval type with an explicit leading precision and the trailing field's
    /// default fractional-seconds precision.
    public static IntervalDayTimeType createIntervalDayTimeType(IntervalField startField, IntervalField endField, int leadingPrecision)
    {
        return createIntervalDayTimeType(startField, endField, leadingPrecision, defaultFractionalPrecision(endField));
    }

    public static IntervalDayTimeType createIntervalDayTimeType(IntervalField startField, IntervalField endField, int leadingPrecision, int fractionalPrecision)
    {
        if (startField.code() > endField.code() || startField.code() < DAY.code() || endField.code() > SECOND.code()) {
            throw new IllegalArgumentException(format("Not a day-time interval qualifier: %s TO %s", startField, endField));
        }
        if (leadingPrecision < 1 || leadingPrecision > MAX_LEADING_PRECISION) {
            throw new IllegalArgumentException(format("INTERVAL leading precision must be in range [1, %s]: %s", MAX_LEADING_PRECISION, leadingPrecision));
        }
        if (fractionalPrecision < 0 || fractionalPrecision > MAX_FRACTIONAL_PRECISION) {
            throw new IllegalArgumentException(format("INTERVAL fractional seconds precision must be in range [0, %s]: %s", MAX_FRACTIONAL_PRECISION, fractionalPrecision));
        }
        return TYPES.computeIfAbsent(key(startField, endField, leadingPrecision, fractionalPrecision), _ ->
                fractionalPrecision <= MAX_SHORT_FRACTIONAL_PRECISION
                        ? new ShortIntervalDayTimeType(startField, endField, leadingPrecision, fractionalPrecision)
                        : new LongIntervalDayTimeType(startField, endField, leadingPrecision, fractionalPrecision));
    }

    IntervalDayTimeType(IntervalField startField, IntervalField endField, int leadingPrecision, int fractionalPrecision, Class<?> javaType, Class<? extends ValueBlock> valueBlockType)
    {
        super(new TypeDescriptor(NAME, numericParameter(startField.code()), numericParameter(endField.code()), numericParameter(leadingPrecision), numericParameter(fractionalPrecision)), javaType, valueBlockType);
        this.startField = startField;
        this.endField = endField;
        this.leadingPrecision = leadingPrecision;
        this.fractionalPrecision = fractionalPrecision;
    }

    public IntervalField getStartField()
    {
        return startField;
    }

    public IntervalField getEndField()
    {
        return endField;
    }

    public int getLeadingPrecision()
    {
        return leadingPrecision;
    }

    public int getFractionalPrecision()
    {
        return fractionalPrecision;
    }

    /// Whether the value is stored in the microsecond-backed short form (a `long`) rather than the
    /// picosecond-backed long form (a [LongInterval]).
    public boolean isShort()
    {
        return fractionalPrecision <= MAX_SHORT_FRACTIONAL_PRECISION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    private static long key(IntervalField startField, IntervalField endField, int leadingPrecision, int fractionalPrecision)
    {
        return ((long) startField.code() << 24) | ((long) endField.code() << 16) | ((long) leadingPrecision << 8) | fractionalPrecision;
    }
}
