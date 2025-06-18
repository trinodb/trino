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
package io.trino.spi.type;

import io.trino.spi.block.Block;

import java.util.Optional;

//
// A date is stored as days from 1970-01-01.
//
// Note: when dealing with a java.sql.Date it is important to remember that the value is stored
// as the number of milliseconds from 1970-01-01T00:00:00 in UTC but time must be midnight in
// the local time zone.  This means when converting between a java.sql.Date and this
// type, the time zone offset must be added or removed to keep the time at midnight in UTC.
//
public final class DateType
        extends AbstractIntType
{
    public static final DateType DATE = new DateType();

    private DateType()
    {
        super(new TypeSignature(StandardTypes.DATE));
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        int days = getInt(block, position);
        return new SqlDate(days);
    }

    @Override
    public Optional<Range> getRange()
    {
        return Optional.of(new Range((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE));
    }

    @Override
    public Optional<Object> getPreviousValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Integer.MIN_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value - 1);
    }

    @Override
    public Optional<Object> getNextValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Integer.MAX_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value + 1);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == DATE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
