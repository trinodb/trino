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
package io.trino.plugin.iceberg;

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static io.trino.plugin.iceberg.PartitionTransforms.epochDay;
import static io.trino.plugin.iceberg.PartitionTransforms.epochHour;
import static io.trino.plugin.iceberg.PartitionTransforms.epochMonth;
import static io.trino.plugin.iceberg.PartitionTransforms.epochYear;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestPartitionTransforms
{
    private static final DateType ICEBERG_DATE = DateType.get();
    private static final TimestampType ICEBERG_TIMESTAMP = TimestampType.withoutZone();

    @Test
    public void testToStringMatchesSpecification()
    {
        assertEquals(Transforms.identity().toString(), "identity");
        assertEquals(Transforms.bucket(13).bind(StringType.get()).toString(), "bucket[13]");
        assertEquals(Transforms.truncate(19).bind(StringType.get()).toString(), "truncate[19]");
        assertEquals(Transforms.year().toString(), "year");
        assertEquals(Transforms.month().toString(), "month");
        assertEquals(Transforms.day().toString(), "day");
        assertEquals(Transforms.hour().toString(), "hour");
    }

    @Test
    public void testEpochTransforms()
    {
        long start = LocalDateTime.of(1965, 10, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC);
        long end = LocalDateTime.of(1974, 3, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC);

        for (long epochSecond = start; epochSecond <= end; epochSecond += 1800) {
            LocalDateTime time = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);

            long epochMilli = SECONDS.toMillis(epochSecond);
            int actualYear = toIntExact(epochYear(epochMilli));
            int actualMonth = toIntExact(epochMonth(epochMilli));
            int actualDay = toIntExact(epochDay(epochMilli));
            int actualHour = toIntExact(epochHour(epochMilli));

            if (time.toLocalTime().equals(LocalTime.MIDNIGHT)) {
                int epochDay = toIntExact(time.toLocalDate().toEpochDay());
                assertEquals(actualYear, (int) Transforms.year().bind(ICEBERG_DATE).apply(epochDay), time.toString());
                assertEquals(actualMonth, (int) Transforms.month().bind(ICEBERG_DATE).apply(epochDay), time.toString());
                assertEquals(actualDay, (int) Transforms.day().bind(ICEBERG_DATE).apply(epochDay), time.toString());
            }

            long epochMicro = SECONDS.toMicros(epochSecond);
            assertEquals(actualYear, (int) Transforms.year().bind(ICEBERG_TIMESTAMP).apply(epochMicro), time.toString());
            assertEquals(actualMonth, (int) Transforms.month().bind(ICEBERG_TIMESTAMP).apply(epochMicro), time.toString());
            assertEquals(actualDay, (int) Transforms.day().bind(ICEBERG_TIMESTAMP).apply(epochMicro), time.toString());
            assertEquals(actualHour, (int) Transforms.hour().bind(ICEBERG_TIMESTAMP).apply(epochMicro), time.toString());
        }
    }
}
