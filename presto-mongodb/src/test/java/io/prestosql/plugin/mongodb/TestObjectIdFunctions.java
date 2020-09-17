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
package io.prestosql.plugin.mongodb;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.time.ZoneOffset.UTC;

public class TestObjectIdFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        functionAssertions.installPlugin(new MongoPlugin());
    }

    @Test
    public void testObjectidTimestamp()
    {
        assertFunction(
                "objectid_timestamp(ObjectId('1234567890abcdef12345678'))",
                TIMESTAMP_TZ_MILLIS,
                toTimestampWithTimeZone(ZonedDateTime.of(1979, 9, 5, 22, 51, 36, 0, UTC)));
    }

    private SqlTimestampWithTimeZone toTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, zonedDateTime.toInstant().toEpochMilli(), 0, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
    }
}
