/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFBinaryFormat;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;

import java.time.ZoneId;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.effectiveSpecializedTimestampFormat;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.fromSqlFormat;

public class StarburstDataConversionContext
        implements DataConversionContext
{
    private final boolean honorClientTZForTimestampNTZ;
    private final SnowflakeDateTimeFormat timestampNTZFormatter;
    private final SnowflakeDateTimeFormat timestampLTZFormatter;
    private final SnowflakeDateTimeFormat timestampTZFormatter;
    private final SnowflakeDateTimeFormat dateFormatter;
    private final SnowflakeDateTimeFormat timeFormatter;
    private final SFBinaryFormat binaryFormatter;
    private final TimeZone timeZone;
    private final int[] scales;
    private final long resultVersion;

    /**
     * Originates from {@link net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1#setupFieldsFromParameters()}
     */
    public StarburstDataConversionContext(SnowflakeSessionParameters parameters, int[] scales, long resultVersion)
    {
        this.scales = requireNonNull(scales, "scales are null");
        this.resultVersion = resultVersion;
        requireNonNull(parameters, "parameters are null");
        String sqlTimestampFormat = parameters.timestampOutputFormat();
        this.timestampNTZFormatter = specializedFormatter(parameters.timestampNtzOutputFormat(), sqlTimestampFormat);
        this.timestampLTZFormatter = specializedFormatter(parameters.timestampLtzOutputFormat(), sqlTimestampFormat);
        this.timestampTZFormatter = specializedFormatter(parameters.timestampTzOutputFormat(), sqlTimestampFormat);
        this.dateFormatter = fromSqlFormat(parameters.dateOutputFormat());
        this.timeFormatter = fromSqlFormat(parameters.timeOutputFormat());
        this.timeZone = TimeZone.getTimeZone(ZoneId.of(parameters.timezone()));
        this.honorClientTZForTimestampNTZ = parameters.honorClientTZForTimestampNTZ();
        this.binaryFormatter = SFBinaryFormat.getSafeOutputFormat(parameters.binaryOutputFormat());
    }

    private static SnowflakeDateTimeFormat specializedFormatter(String defaultFormat, String specializedFormat)
    {
        String sqlFormat = effectiveSpecializedTimestampFormat(specializedFormat, defaultFormat);
        return fromSqlFormat(sqlFormat);
    }

    @Override
    public SnowflakeDateTimeFormat getTimestampLTZFormatter()
    {
        return timestampLTZFormatter;
    }

    @Override
    public SnowflakeDateTimeFormat getTimestampNTZFormatter()
    {
        return timestampNTZFormatter;
    }

    @Override
    public SnowflakeDateTimeFormat getTimestampTZFormatter()
    {
        return timestampTZFormatter;
    }

    @Override
    public SnowflakeDateTimeFormat getDateFormatter()
    {
        return dateFormatter;
    }

    @Override
    public SnowflakeDateTimeFormat getTimeFormatter()
    {
        return timeFormatter;
    }

    @Override
    public SFBinaryFormat getBinaryFormatter()
    {
        return binaryFormatter;
    }

    @Override
    public int getScale(int columnIndex)
    {
        return scales[columnIndex - 1];
    }

    @Override
    public SFBaseSession getSession()
    {
        throw new UnsupportedOperationException("Shouldn't be called, used in tests only");
    }

    @Override
    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    @Override
    public boolean getHonorClientTZForTimestampNTZ()
    {
        return honorClientTZForTimestampNTZ;
    }

    @Override
    public long getResultVersion()
    {
        return resultVersion;
    }
}
