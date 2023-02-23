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

import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.query.QueryAssertions;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.ZonedDateTime;

import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestObjectIdFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new MongoPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testObjectid()
    {
        assertThat(assertions.function("ObjectId", "'1234567890abcdef12345678'"))
                .hasType(OBJECT_ID)
                .isEqualTo(new SqlVarbinary(new ObjectId("1234567890abcdef12345678").toByteArray()));
    }

    @Test
    public void testObjectidIgnoresSpaces()
    {
        assertThat(assertions.function("ObjectId", "'12 34 56 78 90 ab cd ef   12 34 56 78'"))
                .hasType(OBJECT_ID)
                .isEqualTo(new SqlVarbinary(new ObjectId("1234567890abcdef12345678").toByteArray()));
    }

    @Test
    public void testObjectidTimestamp()
    {
        assertThat(assertions.function("objectid_timestamp", "ObjectId('1234567890abcdef12345678')"))
                .hasType(TIMESTAMP_TZ_MILLIS)
                .isEqualTo(toTimestampWithTimeZone(ZonedDateTime.of(1979, 9, 5, 22, 51, 36, 0, UTC)));
    }

    @Test
    public void testTimestampObjectid()
    {
        assertThat(assertions.function("timestamp_objectid", "TIMESTAMP '1979-09-05 22:51:36 +00:00'"))
                .hasType(OBJECT_ID)
                .isEqualTo(new SqlVarbinary(new ObjectId("123456780000000000000000").toByteArray()));
    }

    @Test
    public void testTimestampObjectidNull()
    {
        assertThat(assertions.function("timestamp_objectid", "null"))
                .isNull(OBJECT_ID);
    }

    private SqlTimestampWithTimeZone toTimestampWithTimeZone(ZonedDateTime zonedDateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, zonedDateTime.toInstant().toEpochMilli(), 0, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
    }
}
