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
