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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.analyzer.FeaturesConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;

public class TestMapOperatorsLegacy
        extends AbstractTestFunctions
{
    public TestMapOperatorsLegacy()
    {
        super(new FeaturesConfig().setLegacyRowToJsonCast(true));
    }

    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice uncheckedToJson(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    @Test
    public void testMapToJson()
    {
        // Test key ordering
        assertFunction("CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");

        // Test null value
        assertFunction("cast(cast (null AS MAP(BIGINT, BIGINT)) AS JSON)", JSON, null);
        assertFunction("cast(MAP() AS JSON)", JSON, "{}");
        assertFunction("cast(MAP(ARRAY[1, 2], ARRAY[null, null]) AS JSON)", JSON, "{\"1\":null,\"2\":null}");

        // Test key types
        assertFunction("CAST(MAP(ARRAY[true, false], ARRAY[1, 2]) AS JSON)", JSON, "{\"false\":2,\"true\":1}");

        assertFunction(
                "cast(MAP(cast(ARRAY[1, 2, 3] AS ARRAY(TINYINT)), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"1\":5,\"2\":8,\"3\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[12345, 12346, 12347] AS ARRAY(SMALLINT)), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"12345\":5,\"12346\":8,\"12347\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[123456789,123456790,123456791] AS ARRAY(INTEGER)), ARRAY[5, 8, null]) AS JSON)",
                JSON,
                "{\"123456789\":5,\"123456790\":8,\"123456791\":null}");
        assertFunction(
                "cast(MAP(cast(ARRAY[1234567890123456111,1234567890123456222,1234567890123456777] AS ARRAY(BIGINT)), ARRAY[111, 222, null]) AS JSON)",
                JSON,
                "{\"1234567890123456111\":111,\"1234567890123456222\":222,\"1234567890123456777\":null}");

        assertFunction(
                "cast(MAP(cast(ARRAY[3.14E0, 1e10, 1e20] AS ARRAY(REAL)), ARRAY[null, 10, 20]) AS JSON)",
                JSON,
                "{\"1.0E10\":10,\"1.0E20\":20,\"3.14\":null}");

        assertFunction(
                "cast(MAP(ARRAY[1e-323,1e308,nan()], ARRAY[-323,308,null]) AS JSON)",
                JSON,
                "{\"1.0E-323\":-323,\"1.0E308\":308,\"NaN\":null}");
        assertFunction(
                "cast(MAP(ARRAY[DECIMAL '3.14', DECIMAL '0.01'], ARRAY[0.14, null]) AS JSON)",
                JSON,
                "{\"0.01\":null,\"3.14\":0.14}");

        assertFunction(
                "cast(MAP(ARRAY[DECIMAL '12345678901234567890.1234567890666666', DECIMAL '0.0'], ARRAY[666666, null]) AS JSON)",
                JSON,
                "{\"0.0000000000000000\":null,\"12345678901234567890.1234567890666666\":666666}");

        assertFunction("CAST(MAP(ARRAY['a', 'bb', 'ccc'], ARRAY[1, 2, 3]) AS JSON)", JSON, "{\"a\":1,\"bb\":2,\"ccc\":3}");

        // Test value types
        assertFunction("cast(MAP(ARRAY[1, 2, 3], ARRAY[true, false, null]) AS JSON)", JSON, "{\"1\":true,\"2\":false,\"3\":null}");

        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[5, 8, null] AS ARRAY(TINYINT))) AS JSON)",
                JSON,
                "{\"1\":5,\"2\":8,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[12345, -12345, null] AS ARRAY(SMALLINT))) AS JSON)",
                JSON,
                "{\"1\":12345,\"2\":-12345,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[123456789, -123456789, null] AS ARRAY(INTEGER))) AS JSON)",
                JSON,
                "{\"1\":123456789,\"2\":-123456789,\"3\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY(BIGINT))) AS JSON)",
                JSON,
                "{\"1\":1234567890123456789,\"2\":-1234567890123456789,\"3\":null}");

        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8], CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY(REAL))) AS JSON)",
                JSON,
                "{\"1\":3.14,\"2\":\"NaN\",\"3\":\"Infinity\",\"5\":\"-Infinity\",\"8\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21], ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null]) AS JSON)",
                JSON,
                "{\"1\":3.14,\"13\":\"-Infinity\",\"2\":1.0E-323,\"21\":null,\"3\":1.0E308,\"5\":\"NaN\",\"8\":\"Infinity\"}");
        assertFunction("CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '3.14', null]) AS JSON)", JSON, "{\"1\":3.14,\"2\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '12345678901234567890.123456789012345678', null]) AS JSON)",
                JSON,
                "{\"1\":12345678901234567890.123456789012345678,\"2\":null}");

        assertFunction("cast(MAP(ARRAY[1, 2, 3], ARRAY['a', 'bb', null]) AS JSON)", JSON, "{\"1\":\"a\",\"2\":\"bb\",\"3\":null}");
        assertFunction(
                "CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21, 34], ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null]) AS JSON)",
                JSON,
                "{\"1\":123,\"13\":{\"a\":1,\"b\":\"str\",\"c\":null},\"2\":3.14,\"21\":null,\"3\":false,\"34\":null,\"5\":\"abc\",\"8\":[1,\"a\",null]}");

        assertFunction(
                "CAST(MAP(ARRAY[1, 2], ARRAY[TIMESTAMP '1970-01-01 00:00:01', null]) AS JSON)",
                JSON,
                format("{\"1\":\"%s\",\"2\":null}", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0).toString()));
        assertFunction(
                "CAST(MAP(ARRAY[2, 5, 3], ARRAY[DATE '2001-08-22', DATE '2001-08-23', null]) AS JSON)",
                JSON,
                "{\"2\":\"2001-08-22\",\"3\":null,\"5\":\"2001-08-23\"}");

        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3, 5, 8], ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null]) AS JSON)",
                JSON,
                "{\"1\":[1,2],\"2\":[3,null],\"3\":[],\"5\":[null,null],\"8\":null}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 8, 5, 3], ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null]) AS JSON)",
                JSON,
                "{\"1\":{\"a\":1,\"b\":2},\"2\":{\"none\":null,\"three\":3},\"3\":null,\"5\":{\"h1\":null,\"h2\":null},\"8\":{}}");
        assertFunction(
                "cast(MAP(ARRAY[1, 2, 3, 5], ARRAY[ROW(1, 2), ROW(3, CAST(null AS INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null]) AS JSON)",
                JSON,
                "{\"1\":[1,2],\"2\":[3,null],\"3\":[null,null],\"5\":null}");
        assertFunction("CAST(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) AS JSON)", JSON, "{\"1.00000000000000\":2.2,\"383838383838383.12324234234234\":3.3}");
        assertFunction("CAST(MAP(ARRAY [1.0], ARRAY [2.2]) AS JSON)", JSON, "{\"1.0\":2.2}");
    }
}
