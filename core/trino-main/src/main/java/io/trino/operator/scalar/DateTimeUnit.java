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

import io.airlift.slice.Slice;

import java.util.Optional;

import static java.util.Locale.ENGLISH;

public enum DateTimeUnit
{
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR;

    public static Optional<DateTimeUnit> valueOf(Slice unit, boolean acceptPlural)
    {
        String unitString = unit.toStringUtf8().toUpperCase(ENGLISH);
        if (acceptPlural && unitString.endsWith("S")) {
            unitString = unitString.substring(0, unitString.length() - 1);
        }

        try {
            return Optional.of(valueOf(unitString));
        }
        catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }
}
