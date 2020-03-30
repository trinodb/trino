/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import java.time.Instant;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

public final class LegacyDateTimeConversionUtils
{
    private LegacyDateTimeConversionUtils()
    {
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long toPrestoLegacyTime(long millis, ZoneId sessionZone)
    {
        // the argument represents milliseconds on the epoch day, so we should be able
        // to use the same transformation as for timestamps
        return toPrestoLegacyTimestamp(millis, sessionZone);
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long toPrestoLegacyTimestamp(long millis, ZoneId sessionZone)
    {
        return Instant.ofEpochMilli(millis)
                .atZone(UTC)
                .withZoneSameLocal(sessionZone)
                .toInstant()
                .toEpochMilli();
    }
}
