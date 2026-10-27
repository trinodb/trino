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

import java.time.ZoneId;
import java.util.TreeSet;
import java.util.function.Predicate;

import static io.trino.spi.type.TimeZoneKey.isUtcZoneId;
import static java.lang.Math.abs;
import static java.lang.String.format;

public final class GenerateTimeZoneIndex
{
    private GenerateTimeZoneIndex() {}

    static void main()
            throws InterruptedException
    {
        //
        // Header
        //
        System.out.println("#");
        System.out.println("# DO NOT REMOVE OR MODIFY EXISTING ENTRIES");
        System.out.println("#");
        System.out.println("# This file contains the fixed numeric id of every supported time zone id.");
        System.out.println("# Every zone id in this file must be supported by java.time.ZoneId.");
        System.out.println("#");
        System.out.println("# suppress inspection \"UnusedProperty\" for whole file");

        //
        // We assume 0 is UTC and do not generate it
        //

        //
        // Negative offset
        //
        short nextZoneKey = 1;
        for (int offset = 14 * 60; offset > 0; offset--) {
            String zoneId = format("-%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // Positive offset
        //
        for (int offset = 1; offset <= 14 * 60; offset++) {
            String zoneId = format("+%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // IANA regional zones: region/city
        //

        TreeSet<String> zoneIds = new TreeSet<>(ZoneId.getAvailableZoneIds());
        zoneIds.removeIf(ignoredZone());

        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') < 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        //
        // Other zones
        //
        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') >= 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            System.out.println(zoneKey + " " + zoneId);
        }

        Thread.sleep(1000);
    }

    public static Predicate<String> ignoredZone()
    {
        return zoneId -> isUtcZoneId(zoneId) || zoneId.startsWith("Etc/");
    }
}
