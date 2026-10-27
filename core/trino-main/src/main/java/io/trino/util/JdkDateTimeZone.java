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
package io.trino.util;

import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * Allows Joda-Time calendar arithmetic to use the JDK's time zone database.
 */
final class JdkDateTimeZone
        extends DateTimeZone
{
    private static final DateTimeFormatter SHORT_NAME = DateTimeFormatter.ofPattern("z");
    private static final DateTimeFormatter LONG_NAME = DateTimeFormatter.ofPattern("zzzz");
    private static final Instant MIN_INSTANT = Instant.ofEpochMilli(Long.MIN_VALUE);
    private static final Instant MAX_INSTANT = Instant.ofEpochMilli(Long.MAX_VALUE);

    private final ZoneId zoneId;
    private final ZoneRules rules;

    public JdkDateTimeZone(ZoneId zoneId)
    {
        super(requireNonNull(zoneId, "zoneId is null").getId());
        this.zoneId = zoneId;
        rules = zoneId.getRules();
    }

    @Override
    public String getNameKey(long instant)
    {
        return getShortName(instant, Locale.ENGLISH);
    }

    @Override
    public String getShortName(long instant, Locale locale)
    {
        return SHORT_NAME.withLocale(locale == null ? Locale.getDefault() : locale).withZone(zoneId).format(Instant.ofEpochMilli(instant));
    }

    @Override
    public String getName(long instant, Locale locale)
    {
        return LONG_NAME.withLocale(locale == null ? Locale.getDefault() : locale).withZone(zoneId).format(Instant.ofEpochMilli(instant));
    }

    @Override
    public int getOffset(long instant)
    {
        return rules.getOffset(Instant.ofEpochMilli(instant)).getTotalSeconds() * 1000;
    }

    @Override
    public int getStandardOffset(long instant)
    {
        return rules.getStandardOffset(Instant.ofEpochMilli(instant)).getTotalSeconds() * 1000;
    }

    @Override
    public boolean isFixed()
    {
        return rules.isFixedOffset();
    }

    @Override
    public long nextTransition(long instant)
    {
        ZoneOffsetTransition transition = rules.nextTransition(Instant.ofEpochMilli(instant));
        if (transition == null || transition.getInstant().isAfter(MAX_INSTANT)) {
            return instant;
        }
        return transition.getInstant().toEpochMilli();
    }

    @Override
    public long previousTransition(long instant)
    {
        // Joda-Time includes a transition at the supplied instant and returns the last
        // millisecond before it. ZoneRules.previousTransition is strictly exclusive.
        ZoneOffsetTransition transition = rules.previousTransition(Instant.ofEpochMilli(instant).plusNanos(1));
        if (transition == null || !transition.getInstant().isAfter(MIN_INSTANT)) {
            return instant;
        }
        return transition.getInstant().toEpochMilli() - 1;
    }

    @Override
    public boolean equals(Object object)
    {
        return object instanceof JdkDateTimeZone other && zoneId.equals(other.zoneId);
    }

    @Override
    public int hashCode()
    {
        return zoneId.hashCode();
    }
}
