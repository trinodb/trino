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

import io.trino.spi.block.Block;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.TypeDescriptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.YEAR;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static java.lang.String.format;

/// A year-month interval, stored as a signed count of months.
///
/// The leading and trailing fields are given by an interval qualifier built from
/// [IntervalField#YEAR] and [IntervalField#MONTH], together with the leading field's precision; all three
/// are carried in the type's [TypeDescriptor] so that, for example,
/// `interval year`, `interval month`, `interval year to month`, and
/// `interval year(2)` and `interval year(4)`, are distinct types over the same
/// physical representation.
///
/// The leading precision is the number of decimal digits the leading field may
/// hold. It is always present and at least one; the [field maximum][#maxLeadingPrecision]
/// is the precision sufficient to represent any value the type can store.
public final class IntervalYearMonthType
        extends AbstractIntType
{
    public static final String NAME = INTERVAL_YEAR_TO_MONTH;

    private static final Map<Long, IntervalYearMonthType> TYPES = new ConcurrentHashMap<>();

    /// The widest leading precision the month representation admits, regardless of leading field — the
    /// digit count of the maximum storable magnitude (months). Constructing a type is bounded only by
    /// this structural limit; the tighter field-specific [#maxLeadingPrecision] is a semantic rule the
    /// analyzer and the assignment casts enforce where the SQL specification requires it. Decoupling the
    /// two lets function resolution construct the transient cross-field qualifiers it explores while
    /// binding without the type factory rejecting them outright.
    public static final int MAX_LEADING_PRECISION = 10;

    public static final IntervalYearMonthType INTERVAL_YEAR_MONTH = createIntervalYearMonthType(YEAR, MONTH);

    private final IntervalField startField;
    private final IntervalField endField;
    private final int leadingPrecision;

    /// The largest leading precision a given leading field can hold in the month
    /// representation, i.e. the digit count of its maximum value.
    public static int maxLeadingPrecision(IntervalField field)
    {
        return switch (field) {
            case YEAR -> 9;     // 178956970 years
            case MONTH -> 10;   // 2147483647 months
            default -> throw new IllegalArgumentException("Not a year-month field: " + field);
        };
    }

    /// Creates a year-month interval type whose leading precision is the field maximum — the precision
    /// sufficient to hold any value, used for the canonical type and for derived intervals.
    public static IntervalYearMonthType createIntervalYearMonthType(IntervalField startField, IntervalField endField)
    {
        return createIntervalYearMonthType(startField, endField, maxLeadingPrecision(startField));
    }

    public static IntervalYearMonthType createIntervalYearMonthType(IntervalField startField, IntervalField endField, int leadingPrecision)
    {
        if (startField.code() > endField.code() || startField.code() < YEAR.code() || endField.code() > MONTH.code()) {
            throw new IllegalArgumentException(format("Not a year-month interval qualifier: %s TO %s", startField, endField));
        }
        if (leadingPrecision < 1 || leadingPrecision > MAX_LEADING_PRECISION) {
            throw new IllegalArgumentException(format("INTERVAL leading precision must be in range [1, %s]: %s", MAX_LEADING_PRECISION, leadingPrecision));
        }
        return TYPES.computeIfAbsent(key(startField, endField, leadingPrecision), _ -> new IntervalYearMonthType(startField, endField, leadingPrecision));
    }

    private IntervalYearMonthType(IntervalField startField, IntervalField endField, int leadingPrecision)
    {
        super(new TypeDescriptor(NAME, numericParameter(startField.code()), numericParameter(endField.code()), numericParameter(leadingPrecision)));
        this.startField = startField;
        this.endField = endField;
        this.leadingPrecision = leadingPrecision;
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

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlIntervalYearMonth(getInt(block, position));
    }

    private static long key(IntervalField startField, IntervalField endField, int leadingPrecision)
    {
        return ((long) startField.code() << 16) | ((long) endField.code() << 8) | leadingPrecision;
    }
}
