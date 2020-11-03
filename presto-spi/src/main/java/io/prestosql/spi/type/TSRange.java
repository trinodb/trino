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
package io.prestosql.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import static java.time.ZoneOffset.UTC;

public final class TSRange
{
    public static final DateTimeFormatter JSON_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH[:mm][:ss][.SSS]");

    private final long lower;
    private final long upper;
    private final boolean lowerClosed;
    private final boolean upperClosed;
    private final boolean empty;

    protected static final TSRange EMPTY_RANGE = new TSRange(TSRangeType.EMPTY_VALUE, TSRangeType.EMPTY_VALUE, false, false);

    protected static TSRange empty()
    {
        return TSRangeType.sliceToTSRange(TSRangeType.EMPTY_RANGE_SLICE);
    }

    public static TSRange createTSRange(long lower, long upper, boolean lowerClosed, boolean upperClosed)
    {
        if (lower < 0 || upper < 0) {
            throw new IllegalArgumentException("left and right must not be negative");
        }
        if (lower > upper) {
            throw new IllegalArgumentException("left must not be larger than right");
        }

        if (lower == upper && !(lowerClosed && upperClosed)) {
            return empty();
        }

        return new TSRange(lower, upper, lowerClosed, upperClosed);
    }

    private TSRange(long lower, long upper, boolean lowerClosed, boolean upperClosed)
    {
        this.empty = lower == upper && !(lowerClosed && upperClosed);
        this.lowerClosed = lowerClosed;
        this.lower = empty ? TSRangeType.EMPTY_VALUE : lower;
        this.upper = empty ? TSRangeType.EMPTY_VALUE : upper;
        this.upperClosed = upperClosed;
    }

    public static TSRange fromString(String s)
    {
        if (s == null) {
            throw new IllegalArgumentException("rangeString must not be null");
        }
        else if (s.equals("empty")) {
            return empty();
        }
        final char firstChar = s.charAt(0);
        final char lastChar = s.charAt(s.length() - 1);
        if ((firstChar != '(' && firstChar != '[') || (lastChar != ')' && lastChar != ']')) {
            throw new IllegalArgumentException("Provided string has to start with ( or [ and to end with ) or ]");
        }
        String[] parts = s.substring(1, s.length() - 1).replaceAll("\"", "").split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Lower and Upper Bound of Range String must be separated by ,");
        }
        if (isJsonTimestampFormat(parts[0]) && isJsonTimestampFormat(parts[1])) {
            return fromJson(s);
        }
        try {
            final long lowerEndpoint = Long.parseLong(parts[0]);
            final long upperEndpoint = Long.parseLong(parts[1]);
            final boolean lowerClosed = firstChar == '[';
            final boolean upperClosed = lastChar == ']';
            return createTSRange(lowerEndpoint, upperEndpoint, lowerClosed, upperClosed);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Lower and Upper Bound of Range String must be given as long values");
        }
    }

    @JsonCreator
    public static TSRange fromJson(@JsonProperty(value = "value") String value)
    {
        if (value.equals("empty")) {
            return empty();
        }
        String[] parts = value.replaceAll("\"", "").split(",");
        final boolean leftClosed = parts[0].charAt(0) == '[';
        final boolean rightClosed = parts[1].charAt(parts[1].length() - 1) == ']';
        final long left = LocalDateTime.parse(parts[0].substring(1), JSON_FORMATTER).toInstant(UTC).toEpochMilli();
        final long right = LocalDateTime.parse(parts[1].substring(0, parts[1].length() - 1), JSON_FORMATTER).toInstant(UTC).toEpochMilli();
        return createTSRange(left, right, leftClosed, rightClosed);
    }

    private static boolean isJsonTimestampFormat(String timestampString)
    {
        try {
            JSON_FORMATTER.parse(timestampString);
        }
        catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (isEmpty()) {
            return "empty";
        } // use postgres standard ["lower","upper")
        return String.format("%s\"%s\",\"%s\"%s", lowerClosed ? '[' : '(',
                Instant.ofEpochMilli(lower).atZone(UTC).format(JSON_FORMATTER),
                Instant.ofEpochMilli(upper).atZone(UTC).format(JSON_FORMATTER),
                upperClosed ? ']' : ')');
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lower, lowerClosed, upper, upperClosed);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TSRange other = (TSRange) o;
        if (this.isEmpty() && other.isEmpty()) {
            return true;
        }
        return this.lower == other.lower &&
                this.upper == other.upper &&
                this.lowerClosed == other.lowerClosed &&
                this.upperClosed == other.upperClosed;
    }

    public long getLower()
    {
        return lower;
    }

    public long getUpper()
    {
        return upper;
    }

    public boolean isLowerClosed()
    {
        return lowerClosed;
    }

    public boolean isUpperClosed()
    {
        return upperClosed;
    }

    public boolean isEmpty()
    {
        return empty;
    }
}
