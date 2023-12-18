/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

// TODO: Replace this with UtcTimeZoneCalendar from the Starburst plugins toolkit
/**
 * {@link Calendar} with UTC time zone and no operations besides
 * {@link #getTimeZone()}.
 *
 * <p>Use this calendar in place of a regular {@code Calendar} when only the
 * time zone is needed and the regular {@code Calendar}'s mutability could be a
 * concern.
 */
public class UtcTimeZoneCalendar
        extends Calendar
{
    private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));
    private static final UtcTimeZoneCalendar INSTANCE = new UtcTimeZoneCalendar();

    private UtcTimeZoneCalendar() {}

    public static synchronized UtcTimeZoneCalendar getUtcTimeZoneCalendarInstance()
    {
        return INSTANCE;
    }

    private static RuntimeException unsupported()
    {
        return new UnsupportedOperationException("Operations other than getTimeZone are unsupported.");
    }

    @Override
    protected void computeTime()
    {
        throw unsupported();
    }

    @Override
    protected void computeFields()
    {
        throw unsupported();
    }

    @Override
    public void add(int field, int amount)
    {
        throw unsupported();
    }

    @Override
    public void roll(int field, boolean up)
    {
        throw unsupported();
    }

    @Override
    public int getMinimum(int field)
    {
        throw unsupported();
    }

    @Override
    public int getMaximum(int field)
    {
        throw unsupported();
    }

    @Override
    public int getGreatestMinimum(int field)
    {
        throw unsupported();
    }

    @Override
    public int getLeastMaximum(int field)
    {
        throw unsupported();
    }

    @Override
    public TimeZone getTimeZone()
    {
        return UTC_TIMEZONE;
    }

    @Override
    public int get(int field)
    {
        throw unsupported();
    }

    protected UtcTimeZoneCalendar(TimeZone zone, Locale aLocale)
    {
        throw unsupported();
    }

    @Override
    public long getTimeInMillis()
    {
        throw unsupported();
    }

    @Override
    public void setTimeInMillis(long millis)
    {
        throw unsupported();
    }

    @Override
    public void set(int field, int value)
    {
        throw unsupported();
    }

    @Override
    public String getDisplayName(int field, int style, Locale locale)
    {
        throw unsupported();
    }

    @Override
    public Map<String, Integer> getDisplayNames(int field, int style, Locale locale)
    {
        throw unsupported();
    }

    @Override
    protected void complete()
    {
        throw unsupported();
    }

    @Override
    public String getCalendarType()
    {
        throw unsupported();
    }

    @Override
    public boolean equals(Object obj)
    {
        throw unsupported();
    }

    @Override
    public int hashCode()
    {
        throw unsupported();
    }

    @Override
    public boolean before(Object when)
    {
        throw unsupported();
    }

    @Override
    public boolean after(Object when)
    {
        throw unsupported();
    }

    @Override
    public int compareTo(Calendar anotherCalendar)
    {
        throw unsupported();
    }

    @Override
    public void roll(int field, int amount)
    {
        throw unsupported();
    }

    @Override
    public void setTimeZone(TimeZone value)
    {
        throw unsupported();
    }

    @Override
    public void setLenient(boolean lenient)
    {
        throw unsupported();
    }

    @Override
    public boolean isLenient()
    {
        throw unsupported();
    }

    @Override
    public void setFirstDayOfWeek(int value)
    {
        throw unsupported();
    }

    @Override
    public int getFirstDayOfWeek()
    {
        throw unsupported();
    }

    @Override
    public void setMinimalDaysInFirstWeek(int value)
    {
        throw unsupported();
    }

    @Override
    public int getMinimalDaysInFirstWeek()
    {
        throw unsupported();
    }

    @Override
    public boolean isWeekDateSupported()
    {
        throw unsupported();
    }

    @Override
    public int getWeekYear()
    {
        throw unsupported();
    }

    @Override
    public void setWeekDate(int weekYear, int weekOfYear, int dayOfWeek)
    {
        throw unsupported();
    }

    @Override
    public int getWeeksInWeekYear()
    {
        throw unsupported();
    }

    @Override
    public int getActualMinimum(int field)
    {
        throw unsupported();
    }

    @Override
    public int getActualMaximum(int field)
    {
        throw unsupported();
    }

    @Override
    public Object clone()
    {
        throw unsupported();
    }

    @Override
    public String toString()
    {
        throw unsupported();
    }
}
