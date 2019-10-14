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
package io.prestosql.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.Base64;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.prestosql.client.FixJsonDataUtils.fixData;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.prestosql.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.prestosql.spi.type.StandardTypes.IPADDRESS;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.StandardTypes.UUID;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestFixJsonDataUtils
{
    @Test
    public void testFixData()
    {
        assertQueryResult(BIGINT.getTypeSignature(), 1000, 1000L);
        assertQueryResult(INTEGER.getTypeSignature(), 100, 100);
        assertQueryResult(SMALLINT.getTypeSignature(), 10, (short) 10);
        assertQueryResult(TINYINT.getTypeSignature(), 1, (byte) 1);
        assertQueryResult(BOOLEAN.getTypeSignature(), true, true);
        assertQueryResult(DATE.getTypeSignature(), "2017-07-01", "2017-07-01");
        assertQueryResult(createDecimalType().getTypeSignature(), "2.15", "2.15");
        assertQueryResult(REAL.getTypeSignature(), 100.23456, (float) 100.23456);
        assertQueryResult(DOUBLE.getTypeSignature(), 100.23456D, 100.23456);
        assertQueryResult(new TypeSignature(INTERVAL_DAY_TO_SECOND), "INTERVAL '2' DAY", "INTERVAL '2' DAY");
        assertQueryResult(new TypeSignature(INTERVAL_YEAR_TO_MONTH), "INTERVAL '3' MONTH", "INTERVAL '3' MONTH");
        assertQueryResult(TIMESTAMP.getTypeSignature(), "2001-08-22 03:04:05.321", "2001-08-22 03:04:05.321");
        assertQueryResult(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature(), "2001-08-22 03:04:05.321 America/Los_Angeles", "2001-08-22 03:04:05.321 America/Los_Angeles");
        assertQueryResult(TIME.getTypeSignature(), "01:02:03.456", "01:02:03.456");
        assertQueryResult(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature(), "01:02:03.456 America/Los_Angeles", "01:02:03.456 America/Los_Angeles");
        assertQueryResult(VARBINARY.getTypeSignature(), "garbage", Base64.getDecoder().decode("garbage"));
        assertQueryResult(VARCHAR.getTypeSignature(), "teststring", "teststring");
        assertQueryResult(createCharType(3).getTypeSignature(), "abc", "abc");
        assertQueryResult(
                RowType.from(ImmutableList.of(
                        field("foo", BIGINT),
                        field("bar", BIGINT))).getTypeSignature(),
                ImmutableList.of(1, 2),
                ImmutableMap.of("foo", 1L, "bar", 2L));
        assertQueryResult(arrayType(BIGINT.getTypeSignature()), ImmutableList.of(1, 2, 4), ImmutableList.of(1L, 2L, 4L));
        assertQueryResult(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()), ImmutableMap.of(1, 3, 2, 4), ImmutableMap.of(1L, 3L, 2L, 4L));
        assertQueryResult(new TypeSignature(JSON), "{\"json\": {\"a\": 1}}", "{\"json\": {\"a\": 1}}");
        assertQueryResult(new TypeSignature(IPADDRESS), "1.2.3.4", "1.2.3.4");
        assertQueryResult(new TypeSignature(UUID), "0397e63b-2b78-4b7b-9c87-e085fa225dd8", "0397e63b-2b78-4b7b-9c87-e085fa225dd8");
        assertQueryResult(new TypeSignature("Geometry"), "POINT (1.2 3.4)", "POINT (1.2 3.4)");
        assertQueryResult(
                mapType(new TypeSignature("BingTile"), BIGINT.getTypeSignature()),
                ImmutableMap.of("BingTile{x=1, y=2, zoom_level=10}", 1),
                ImmutableMap.of("BingTile{x=1, y=2, zoom_level=10}", 1L));
    }

    private void assertQueryResult(TypeSignature type, Object data, Object expected)
    {
        List<List<Object>> rows = newArrayList(fixData(
                ImmutableList.of(new Column("test", type.toString(), toClientTypeSignature(type))),
                ImmutableList.of(ImmutableList.of(data))));
        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).size(), 1);
        assertEquals(rows.get(0).get(0), expected);
    }

    private static ClientTypeSignature toClientTypeSignature(TypeSignature signature)
    {
        return new ClientTypeSignature(signature.getBase(), signature.getParameters().stream()
                .map(TestFixJsonDataUtils::toClientTypeSignatureParameter)
                .collect(toImmutableList()));
    }

    private static ClientTypeSignatureParameter toClientTypeSignatureParameter(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return ClientTypeSignatureParameter.ofType(toClientTypeSignature(parameter.getTypeSignature()));
            case NAMED_TYPE:
                return ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(
                        parameter.getNamedTypeSignature().getFieldName().map(value ->
                                new RowFieldName(value.getName(), value.isDelimited())),
                        toClientTypeSignature(parameter.getNamedTypeSignature().getTypeSignature())));
            case LONG:
                return ClientTypeSignatureParameter.ofLong(parameter.getLongLiteral());
        }
        throw new IllegalArgumentException("Unsupported kind: " + parameter.getKind());
    }
}
