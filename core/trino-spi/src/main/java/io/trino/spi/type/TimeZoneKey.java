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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.TrinoException;

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

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
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
            "GMT",
            "GMT0",
            "GMT+0",
            "GMT-0",
            "Etc/GMT",
            "Etc/GMT0",
            "Etc/GMT+0",
            "Etc/GMT-0",
            "UT",
            "UT+0",
            "UT-0",
            "Etc/UT",
            "Etc/UT+0",
            "Etc/UT-0",
            "UTC",
            "UTC+0",
            "UTC-0",
            "Etc/UTC",
            "Etc/UTC+0",
            "Etc/UTC-0",
            "+0000",
            "+00:00",
            "-0000",
            "-00:00",
            "Z",
            "Zulu",
            "UCT",
            "Greenwich",
            "Universal",
            "Etc/Universal",
            "Etc/UCT");

    private static final List<String> UTC_OFFSET_PREFIXES = List.of(
            "Etc/GMT",
            "Etc/UTC",
            "Etc/UT",
            "GMT",
            "UTC",
            "UT");

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
            zoneIdToKey.put(UTC_KEY.getId(), UTC_KEY);

            short maxZoneKey = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                short zoneKey = Short.parseShort(((String) entry.getKey()).trim());
                String zoneId = ((String) entry.getValue()).trim();

                maxZoneKey = (short) max(maxZoneKey, zoneKey);
                zoneIdToKey.put(zoneId, new TimeZoneKey(zoneId, zoneKey));
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

    /**
     * @throws TimeZoneNotSupportedException when {@code zoneId} does not identity a time zone
     */
    public static TimeZoneKey getTimeZoneKey(String zoneId)
    {
        requireNonNull(zoneId, "zoneId is null");
        checkArgument(!zoneId.isEmpty(), "Zone id is an empty string");

        TimeZoneKey zoneKey = ZONE_ID_TO_KEY.get(zoneId);
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
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid offset minutes " + offsetMinutes);
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
        return normalizeZoneId(zoneId).equals("UTC");
    }

    private static String normalizeZoneId(String zoneId)
    {
        if (isUtcEquivalentName(zoneId)) {
            return "UTC";
        }

        // The JDK doesn't understand legacy timezones such as Etc/GMT+00:00, GMT-7
        for (String prefix : UTC_OFFSET_PREFIXES) {
            if (zoneId.startsWith(prefix)) {
                int offset = prefix.length();
                if (offset == zoneId.length() || zoneId.charAt(offset) != '+' && zoneId.charAt(offset) != '-') {
                    // It must be something like GMT7, or Etc/UTC4, which are invalid. Unsupported timezones are handled by the caller.
                    return zoneId;
                }

                int colon = zoneId.indexOf(':', offset);
                int hour;
                int minute = 0;
                try {
                    if (colon != -1) {
                        hour = Integer.parseInt(zoneId, offset, colon, 10);
                        minute = Integer.parseInt(zoneId, colon + 1, zoneId.length(), 10);
                    }
                    else if (zoneId.length() - offset <= 3) {
                        // +H or +HH
                        hour = Integer.parseInt(zoneId, offset, zoneId.length(), 10);
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
                    return "UTC";
                }

                if (prefix.equals("Etc/GMT")) {
                    hour = -hour;
                }

                return formatZoneOffset(hour >= 0, abs(hour), minute);
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
            return "UTC";
        }

        return zoneId;
    }

    private static String formatZoneOffset(boolean positive, int hour, int minute)
    {
        StringBuilder builder = new StringBuilder(6);

        builder.append(positive ? '+' : '-');

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
        return UTC_EQUIVALENTS.contains(zoneId);
    }

    private static String zoneIdForOffset(long offsetMinutes)
    {
        return formatZoneOffset(offsetMinutes >= 0, toIntExact(abs(offsetMinutes / 60)), (int) abs(offsetMinutes % 60));
    }

    private static void checkArgument(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new IllegalArgumentException(format(message, args));
        }
    }
}
