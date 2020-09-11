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
package io.prestosql.plugin.iceberg;

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.iceberg.PartitionTransforms.epochMonth;
import static io.prestosql.plugin.iceberg.PartitionTransforms.epochYear;
import static org.testng.Assert.assertEquals;

public class TestPartitionTransforms
{
    @Test
    public void testToStringMatchesSpecification()
    {
        assertEquals(Transforms.identity(StringType.get()).toString(), "identity");
        assertEquals(Transforms.bucket(StringType.get(), 13).toString(), "bucket[13]");
        assertEquals(Transforms.truncate(StringType.get(), 19).toString(), "truncate[19]");
        assertEquals(Transforms.year(DateType.get()).toString(), "year");
        assertEquals(Transforms.month(DateType.get()).toString(), "month");
        assertEquals(Transforms.day(DateType.get()).toString(), "day");
        assertEquals(Transforms.hour(TimestampType.withoutZone()).toString(), "hour");
    }

    @Test
    public void testEpochYear()
    {
        assertEquals(epochYear(dateMillis(1965, 1, 1)), -5);
        assertEquals(epochYear(dateMillis(1965, 6, 3)), -5);
        assertEquals(epochYear(dateMillis(1965, 12, 31)), -5);
        assertEquals(epochYear(dateMillis(1969, 1, 1)), -1);
        assertEquals(epochYear(dateMillis(1969, 7, 20)), -1);
        assertEquals(epochYear(dateMillis(1969, 12, 31)), -1);
        assertEquals(epochYear(dateMillis(1970, 1, 1)), 0);
        assertEquals(epochYear(dateMillis(1970, 12, 31)), 0);
        assertEquals(epochYear(dateMillis(2012, 8, 20)), 42);
    }

    @Test
    public void testEpochMonth()
    {
        assertEquals(epochMonth(dateMillis(1965, 1, 1)), -60);
        assertEquals(epochMonth(dateMillis(1965, 6, 3)), -55);
        assertEquals(epochMonth(dateMillis(1965, 12, 31)), -49);
        assertEquals(epochMonth(dateMillis(1969, 1, 1)), -12);
        assertEquals(epochMonth(dateMillis(1969, 7, 20)), -6);
        assertEquals(epochMonth(dateMillis(1969, 12, 31)), -1);
        assertEquals(epochMonth(dateMillis(1970, 1, 1)), 0);
        assertEquals(epochMonth(dateMillis(1970, 12, 31)), 11);
        assertEquals(epochMonth(dateMillis(2012, 8, 20)), 511);
    }

    private static long dateMillis(int year, int month, int day)
    {
        return TimeUnit.SECONDS.toMillis(LocalDateTime.of(year, month, day, 0, 0, 0).toEpochSecond(ZoneOffset.UTC));
    }
}
