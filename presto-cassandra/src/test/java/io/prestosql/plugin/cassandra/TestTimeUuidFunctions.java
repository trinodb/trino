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
package io.prestosql.plugin.cassandra;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;

public class TestTimeUuidFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        functionAssertions.installPlugin(new CassandraPlugin());
    }

    @Test
    public void testMinTimeuuid()
    {
        assertFunction("min_timeuuid(TIMESTAMP '2013-01-01 00:05:00.000')", createVarcharType(36), "89816e00-5331-11e2-8080-808080808080");
        assertFunction("min_timeuuid(TIMESTAMP '2013-02-02 10:00:00.000')", createVarcharType(36), "f5952000-6ca9-11e2-8080-808080808080");

        assertInvalidFunction("min_timeuuid(TIMESTAMP '2013-02-02 10:00:00.1234')", "Millisecond precision should be lower than or equal to 3");
    }

    @Test
    public void testMaxTimeuuid()
    {
        assertFunction("max_timeuuid(TIMESTAMP '2013-01-01 00:05:00.000')", createVarcharType(36), "8981950f-5331-11e2-7f7f-7f7f7f7f7f7f");
        assertFunction("max_timeuuid(TIMESTAMP '2013-02-02 10:00:00.000')", createVarcharType(36), "f595470f-6ca9-11e2-7f7f-7f7f7f7f7f7f");

        assertInvalidFunction("max_timeuuid(TIMESTAMP '2013-02-02 10:00:00.1234')", "Millisecond precision should be lower than or equal to 3");
    }

    @Test
    public void testTimeuuidToTimestamp()
    {
        assertFunction(
                "timeuuid_to_timestamp('8981950f-5331-11e2-7f7f-7f7f7f7f7f7f')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2012, 12, 31, 10, 05, 00, 0, UTC)));
        assertFunction(
                "timeuuid_to_timestamp('f5952000-6ca9-11e2-8080-808080808080')",
                createTimestampWithTimeZoneType(3),
                toTimestampWithTimeZone(ZonedDateTime.of(2013, 2, 1, 20, 00, 00, 0, UTC)));
    }

    private static SqlTimestampWithTimeZone toTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, zonedDateTime.toInstant().toEpochMilli(), 0, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
    }
}
