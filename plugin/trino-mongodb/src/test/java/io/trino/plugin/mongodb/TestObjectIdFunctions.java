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
package io.trino.plugin.mongodb;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeZoneKey;
import org.bson.types.ObjectId;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.time.ZoneOffset.UTC;

public class TestObjectIdFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void registerFunctions()
    {
        functionAssertions.installPlugin(new MongoPlugin());
    }

    @Test
    public void testObjectid()
    {
        assertFunction(
                "ObjectId('1234567890abcdef12345678')",
                OBJECT_ID,
                new SqlVarbinary(new ObjectId("1234567890abcdef12345678").toByteArray()));
    }

    @Test
    public void testObjectidIgnoresSpaces()
    {
        assertFunction(
                "ObjectId('12 34 56 78 90 ab cd ef   12 34 56 78')",
                OBJECT_ID,
                new SqlVarbinary(new ObjectId("1234567890abcdef12345678").toByteArray()));
    }

    @Test
    public void testObjectidTimestamp()
    {
        assertFunction(
                "objectid_timestamp(ObjectId('1234567890abcdef12345678'))",
                TIMESTAMP_TZ_MILLIS,
                toTimestampWithTimeZone(ZonedDateTime.of(1979, 9, 5, 22, 51, 36, 0, UTC)));
    }

    @Test
    public void testTimestampObjectid()
    {
        assertFunction(
                "timestamp_objectid(TIMESTAMP '1979-09-05 22:51:36 +00:00')",
                OBJECT_ID,
                new SqlVarbinary(new ObjectId("123456780000000000000000").toByteArray()));
    }

    @Test
    public void testTimestampObjectidNull()
    {
        assertFunction(
                "timestamp_objectid(null)",
                OBJECT_ID,
                null);
    }

    private SqlTimestampWithTimeZone toTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, zonedDateTime.toInstant().toEpochMilli(), 0, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
    }
}
