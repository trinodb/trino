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
package io.trino.plugin.cassandra;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.UuidType.UUID;
import static java.time.ZoneOffset.UTC;

public class TestTimeUuidFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void registerFunctions()
    {
        functionAssertions.installPlugin(new CassandraPlugin());
    }

    @Test
    public void testMinTimeuuid()
    {
        assertFunction("min_timeuuid(TIMESTAMP '2021-01-02 12:34:56.999 UTC')", UUID, "ebff8770-4cf6-11eb-8080-808080808080");
        assertFunction("min_timeuuid(TIMESTAMP '2021-01-02 12:34:56.999+01:00')", UUID, "8a3b1f70-4cee-11eb-8080-808080808080");

        assertFunction("min_timeuuid(TIMESTAMP '2013-01-01 00:05:00.9 Pacific/Apia')", UUID, "8a0ac240-5331-11e2-8080-808080808080");
        assertFunction("min_timeuuid(TIMESTAMP '2013-01-01 00:05:00.99 Pacific/Apia')", UUID, "8a187de0-5331-11e2-8080-808080808080");
        assertFunction("min_timeuuid(TIMESTAMP '2013-01-01 00:05:00.999 Pacific/Apia')", UUID, "8a19dd70-5331-11e2-8080-808080808080");

        assertFunction("min_timeuuid(null)", UUID, null);

        assertInvalidFunction("min_timeuuid(TIMESTAMP '2013-02-02 10:00:00.1234')", "Precision should be lower than or equal to 3");
    }

    @Test
    public void testMaxTimeuuid()
    {
        assertFunction("max_timeuuid(TIMESTAMP '2021-01-02 12:34:56.999 UTC')", UUID, "ebffae7f-4cf6-11eb-7f7f-7f7f7f7f7f7f");
        assertFunction("max_timeuuid(TIMESTAMP '2021-01-02 12:34:56.999+01:00')", UUID, "8a3b467f-4cee-11eb-7f7f-7f7f7f7f7f7f");

        assertFunction("max_timeuuid(TIMESTAMP '2013-01-01 00:05:00.9 Pacific/Apia')", UUID, "8a0ae94f-5331-11e2-7f7f-7f7f7f7f7f7f");
        assertFunction("max_timeuuid(TIMESTAMP '2013-01-01 00:05:00.99 Pacific/Apia')", UUID, "8a18a4ef-5331-11e2-7f7f-7f7f7f7f7f7f");
        assertFunction("max_timeuuid(TIMESTAMP '2013-01-01 00:05:00.999 Pacific/Apia')", UUID, "8a1a047f-5331-11e2-7f7f-7f7f7f7f7f7f");

        assertFunction("max_timeuuid(null)", UUID, null);

        assertInvalidFunction("max_timeuuid(TIMESTAMP '2013-02-02 10:00:00.1234')", "Precision should be lower than or equal to 3");
    }

    @Test
    public void testTimeuuidToTimestamp()
    {
        assertFunction(
                "timeuuid_to_timestamp(UUID 'ebff8770-4cf6-11eb-8080-808080808080')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2021, 1, 2, 12, 34, 56, 999000000, UTC)));
        assertFunction(
                "timeuuid_to_timestamp(UUID '8a3b1f70-4cee-11eb-8080-808080808080')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2021, 1, 2, 11, 34, 56, 999000000, UTC)));
        assertFunction(
                "timeuuid_to_timestamp(UUID 'ebffae7f-4cf6-11eb-7f7f-7f7f7f7f7f7f')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2021, 1, 2, 12, 34, 56, 999000000, UTC)));
        assertFunction(
                "timeuuid_to_timestamp(UUID '8a3b467f-4cee-11eb-7f7f-7f7f7f7f7f7f')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2021, 1, 2, 11, 34, 56, 999000000, UTC)));
        assertFunction(
                "timeuuid_to_timestamp(null)",
                createTimestampWithTimeZoneType(3),
                null);
    }

    private static SqlTimestampWithTimeZone toTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, zonedDateTime.toInstant().toEpochMilli(), 0, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
    }
}
