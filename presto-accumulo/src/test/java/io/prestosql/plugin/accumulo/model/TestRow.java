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
package io.prestosql.plugin.accumulo.model;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.GregorianCalendar;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestRow
{
    @Test
    public void testRow()
    {
        Row r1 = new Row();
        r1.addField(new Field(AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c")), new ArrayType(VARCHAR)));
        r1.addField(true, BOOLEAN);
        r1.addField(new Field(10592L, DATE));
        r1.addField(123.45678, DOUBLE);
        r1.addField(new Field((long) floatToIntBits(123.45678f), REAL));
        r1.addField(12345678L, INTEGER);
        r1.addField(new Field(12345678L, BIGINT));
        r1.addField(new Field(12345L, SMALLINT));
        r1.addField(new GregorianCalendar(1970, 0, 1, 12, 30, 0).getTime().getTime(), TIME);
        r1.addField(new Field(Timestamp.valueOf(LocalDateTime.of(1999, 1, 1, 12, 30, 0)).getTime(), TIMESTAMP_MILLIS));
        r1.addField((long) 123, TINYINT);
        r1.addField(new Field(Slices.wrappedBuffer("O'Leary".getBytes(UTF_8)), VARBINARY));
        r1.addField(utf8Slice("O'Leary"), VARCHAR);
        r1.addField(null, VARCHAR);

        assertEquals(r1.length(), 14);

        Row r2 = new Row(r1);
        assertEquals(r2, r1);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testRowTypeIsNull()
    {
        Row r1 = new Row();
        r1.addField(VARCHAR, null);
    }
}
