/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.presto.plugin.snowflake.jdbc;

import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

class UtcTimeZoneCalendar
        extends Calendar
{
    /**
     * A {@link Calendar} subclass which supports {@link Calendar#getTimeZone} only.
     */

    private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
    private static UtcTimeZoneCalendar utcTimeZoneCalendarInstance;

    private UtcTimeZoneCalendar()
    {
    }

    public static synchronized UtcTimeZoneCalendar getUtcTimeZoneCalendarInstance()
    {
        if (utcTimeZoneCalendarInstance == null) {
            utcTimeZoneCalendarInstance = new UtcTimeZoneCalendar();
        }
        return utcTimeZoneCalendarInstance;
    }

    @Override
    protected void computeTime()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    protected void computeFields()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void add(int field, int amount)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void roll(int field, boolean up)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getMinimum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getMaximum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getGreatestMinimum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getLeastMaximum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public TimeZone getTimeZone()
    {
        return UTC_TIMEZONE;
    }

    @Override
    public int get(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    protected UtcTimeZoneCalendar(TimeZone zone, Locale aLocale)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public long getTimeInMillis()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setTimeInMillis(long millis)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void set(int field, int value)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public String getDisplayName(int field, int style, Locale locale)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public Map<String, Integer> getDisplayNames(int field, int style, Locale locale)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    protected void complete()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public String getCalendarType()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public boolean equals(Object obj)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public boolean before(Object when)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public boolean after(Object when)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int compareTo(Calendar anotherCalendar)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void roll(int field, int amount)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setTimeZone(TimeZone value)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setLenient(boolean lenient)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public boolean isLenient()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setFirstDayOfWeek(int value)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getFirstDayOfWeek()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setMinimalDaysInFirstWeek(int value)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getMinimalDaysInFirstWeek()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public boolean isWeekDateSupported()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getWeekYear()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public void setWeekDate(int weekYear, int weekOfYear, int dayOfWeek)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getWeeksInWeekYear()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getActualMinimum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public int getActualMaximum(int field)
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public Object clone()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }

    @Override
    public String toString()
    {
        throw new UnsupportedOperationException("Operations other than getTimezone are unsupported.");
    }
}
