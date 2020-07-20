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
import com.fasterxml.jackson.annotation.JsonValue;
import io.prestosql.spi.PrestoException;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class TimeZoneKey
{
    public static final TimeZoneKey UTC_KEY = new TimeZoneKey("UTC", (short) 0);
    public static final short MAX_TIME_ZONE_KEY;
    private static final Map<String, TimeZoneKey> ZONE_ID_TO_KEY;
    private static final Set<TimeZoneKey> ZONE_KEYS;

    private static final TimeZoneKey[] TIME_ZONE_KEYS;

    private static final short OFFSET_TIME_ZONE_MIN = -14 * 60;
    private static final short OFFSET_TIME_ZONE_MAX = 14 * 60;
    private static final TimeZoneKey[] OFFSET_TIME_ZONE_KEYS = new TimeZoneKey[OFFSET_TIME_ZONE_MAX - OFFSET_TIME_ZONE_MIN + 1];

    private static final Set<String> UTC_EQUIVALENTS = Set.of(
            "gmt",
            "gmt0",
            "gmt+0",
            "gmt-0",
            "etc/gmt",
            "etc/gmt0",
            "etc/gmt+0",
            "etc/gmt-0",
            "ut",
            "ut+0",
            "ut-0",
            "etc/ut",
            "etc/ut+0",
            "etc/ut-0",
            "utc",
            "utc+0",
            "utc-0",
            "etc/utc",
            "etc/utc+0",
            "etc/utc-0",
            "+0000",
            "+00:00",
            "-0000",
            "-00:00",
            "z",
            "zulu",
            "uct",
            "greenwich",
            "universal",
            "etc/universal",
            "etc/uct");

    private static final List<String> UTC_OFFSET_PREFIXES = List.of(
            "etc/gmt",
            "etc/utc",
            "etc/ut",
            "gmt",
            "utc",
            "ut");

    static {
        try (InputStream in = TimeZoneKey.class.getResourceAsStream("zone-index.properties")) {
            // load zone file
            // todo parse file by hand since Properties ignores duplicate entries
            Properties data = new Properties()
            {
                @Override
                public synchronized Object put(Object key, Object value)
                {
                    Object existingEntry = super.put(key, value);
                    if (existingEntry != null) {
                        throw new AssertionError("Zone file has duplicate entries for " + key);
                    }
                    return null;
                }
            };
            data.load(in);

            if (data.containsKey("0")) {
                throw new AssertionError("Zone file should not contain a mapping for key 0");
            }

            Map<String, TimeZoneKey> zoneIdToKey = new TreeMap<>();
            zoneIdToKey.put(UTC_KEY.getId().toLowerCase(ENGLISH), UTC_KEY);

            short maxZoneKey = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                short zoneKey = Short.valueOf(((String) entry.getKey()).trim());
                String zoneId = ((String) entry.getValue()).trim();

                maxZoneKey = (short) max(maxZoneKey, zoneKey);
                zoneIdToKey.put(zoneId.toLowerCase(ENGLISH), new TimeZoneKey(zoneId, zoneKey));
            }

            MAX_TIME_ZONE_KEY = maxZoneKey;
            ZONE_ID_TO_KEY = Collections.unmodifiableMap(new LinkedHashMap<>(zoneIdToKey));
            ZONE_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(zoneIdToKey.values()));

            TIME_ZONE_KEYS = new TimeZoneKey[maxZoneKey + 1];
            for (TimeZoneKey timeZoneKey : zoneIdToKey.values()) {
                TIME_ZONE_KEYS[timeZoneKey.getKey()] = timeZoneKey;
            }

            for (short offset = OFFSET_TIME_ZONE_MIN; offset <= OFFSET_TIME_ZONE_MAX; offset++) {
                if (offset == 0) {
                    continue;
                }
                String zoneId = zoneIdForOffset(offset);
                TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId);
                OFFSET_TIME_ZONE_KEYS[offset - OFFSET_TIME_ZONE_MIN] = zoneKey;
            }
        }
        catch (IOException e) {
            throw new AssertionError("Error loading time zone index file", e);
        }
    }

    public static Set<TimeZoneKey> getTimeZoneKeys()
    {
        return ZONE_KEYS;
    }

    @JsonCreator
    public static TimeZoneKey getTimeZoneKey(short timeZoneKey)
    {
        checkArgument(timeZoneKey < TIME_ZONE_KEYS.length && TIME_ZONE_KEYS[timeZoneKey] != null, "Invalid time zone key %s", timeZoneKey);
        return TIME_ZONE_KEYS[timeZoneKey];
    }

    public static TimeZoneKey getTimeZoneKey(String zoneId)
    {
        requireNonNull(zoneId, "Zone id is null");
        checkArgument(!zoneId.isEmpty(), "Zone id is an empty string");

        TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId.toLowerCase(ENGLISH));
        if (zoneKey == null) {
            zoneKey = ZONE_ID_TO_KEY.get(normalizeZoneId(zoneId));
        }
        if (zoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneId);
        }
        return zoneKey;
    }

    public static TimeZoneKey getTimeZoneKeyForOffset(long offsetMinutes)
    {
        if (offsetMinutes == 0) {
            return UTC_KEY;
        }

        if (!(offsetMinutes >= OFFSET_TIME_ZONE_MIN && offsetMinutes <= OFFSET_TIME_ZONE_MAX)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid offset minutes " + offsetMinutes);
        }
        TimeZoneKey timeZoneKey = OFFSET_TIME_ZONE_KEYS[((int) offsetMinutes) - OFFSET_TIME_ZONE_MIN];
        if (timeZoneKey == null) {
            throw new TimeZoneNotSupportedException(zoneIdForOffset(offsetMinutes));
        }
        return timeZoneKey;
    }

    private final String id;

    private final short key;

    TimeZoneKey(String id, short key)
    {
        this.id = requireNonNull(id, "id is null");
        if (key < 0) {
            throw new IllegalArgumentException("key is negative");
        }
        this.key = key;
    }

    public String getId()
    {
        return id;
    }

    public ZoneId getZoneId()
    {
        return ZoneId.of(id);
    }

    @JsonValue
    public short getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, key);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeZoneKey other = (TimeZoneKey) obj;
        return Objects.equals(this.id, other.id) && Objects.equals(this.key, other.key);
    }

    @Override
    public String toString()
    {
        return id;
    }

    public static boolean isUtcZoneId(String zoneId)
    {
        return normalizeZoneId(zoneId).equals("utc");
    }

    private static String normalizeZoneId(String zoneId)
    {
        if (isUtcEquivalentName(zoneId)) {
            return "utc";
        }

        // The JDK doesn't understand legacy timezones such as Etc/GMT+00:00, GMT-7
        String lowerCased = zoneId.toLowerCase(ENGLISH);
        for (String prefix : UTC_OFFSET_PREFIXES) {
            if (lowerCased.startsWith(prefix)) {
                int offset = prefix.length();
                if (offset == lowerCased.length() || lowerCased.charAt(offset) != '+' && lowerCased.charAt(offset) != '-') {
                    // It must be something like GMT7, or Etc/UTC4, which are invalid. Unsupported timezones are handled by the caller.
                    return zoneId;
                }

                int colon = lowerCased.indexOf(':', offset);
                int hour;
                int minute = 0;
                try {
                    if (colon != -1) {
                        hour = Integer.parseInt(lowerCased, offset, colon, 10);
                        minute = Integer.parseInt(lowerCased, colon + 1, lowerCased.length(), 10);
                    }
                    else if (lowerCased.length() - offset <= 3) {
                        // +H or +HH
                        hour = Integer.parseInt(lowerCased, offset, lowerCased.length(), 10);
                    }
                    else {
                        // let the JDK handle it below
                        break;
                    }
                }
                catch (NumberFormatException e) {
                    // Invalid time zone is handled by the caller
                    return zoneId;
                }

                if (hour == 0 && minute == 0) {
                    return "utc";
                }

                if (prefix.equals("etc/gmt")) {
                    hour = -hour;
                }

                return formatZoneOffset(hour, minute);
            }
        }

        // Normalize using JDK rules. In particular, normalize forms such as +0735, +7, +073521
        try {
            zoneId = ZoneId.of(zoneId).getId();
        }
        catch (Exception e) {
            return zoneId;
        }

        if (isUtcEquivalentName(zoneId)) {
            return "utc";
        }

        return zoneId;
    }

    private static String formatZoneOffset(int hour, int minute)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(hour >= 0 ? '+' : '-');

        hour = abs(hour);
        if (hour < 10) {
            builder.append('0');
        }
        builder.append(hour);
        builder.append(':');

        if (minute < 10) {
            builder.append('0');
        }
        builder.append(minute);

        return builder.toString();
    }

    private static boolean isUtcEquivalentName(String zoneId)
    {
        return UTC_EQUIVALENTS.contains(zoneId.toLowerCase(ENGLISH));
    }

    private static String zoneIdForOffset(long offset)
    {
        return format("%s%02d:%02d", offset < 0 ? "-" : "+", abs(offset / 60), abs(offset % 60));
    }

    private static void checkArgument(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new IllegalArgumentException(format(message, args));
        }
    }
}
