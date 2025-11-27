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
package io.trino.plugin.clickhouse;

import io.trino.spi.type.DecimalType;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.ALTINITY_DEFAULT_IMAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;

final class TestAltinityClickHouseTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(ALTINITY_DEFAULT_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer).build();
    }

    @Override
    protected SqlDataTypeTest addClickhouseCreateAndInsertDataTypeTests(SqlDataTypeTest dataTypeTest)
    {
        // Older versions of Altinity are missing a String -> IPADDRESS cast which results in the test setup for those tests failing
        dataTypeTest.addRoundTrip("Map(Int32, Bool)", "map(42, true)", mapWithValueType(BOOLEAN), "MAP(ARRAY[42], ARRAY[true])")
                .addRoundTrip("Map(Int32, Int8)", "map(42, 0)", mapWithValueType(TINYINT), "MAP(ARRAY[42], ARRAY[TINYINT '0'])")
                .addRoundTrip("Map(Int32, Int32)", "map(42, 1)", mapWithValueType(INTEGER), "MAP(ARRAY[42], ARRAY[1])")
                .addRoundTrip("Map(Int32, Int64)", "map(42, 2)", mapWithValueType(BIGINT), "MAP(ARRAY[42], ARRAY[BIGINT '2'])")
                .addRoundTrip("Map(Int32, UInt8)", "map(42, 3)", mapWithValueType(SMALLINT), "MAP(ARRAY[42], ARRAY[SMALLINT '3'])")
                .addRoundTrip("Map(Int32, UInt16)", "map(42, 4)", mapWithValueType(INTEGER), "MAP(ARRAY[42], ARRAY[4])")
                .addRoundTrip("Map(Int32, UInt32)", "map(42, 5)", mapWithValueType(BIGINT), "MAP(ARRAY[42], ARRAY[BIGINT '5'])")
                .addRoundTrip("Map(Int32, UInt64)", "map(42, 6)", mapWithValueType(DecimalType.createDecimalType(20, 0)), "MAP(ARRAY[42], ARRAY[CAST(6 AS DECIMAL(20, 0))])")
                .addRoundTrip("Map(Int32, Float32)", "map(42, 7)", mapWithValueType(REAL), "MAP(ARRAY[42], ARRAY[REAL '7'])")
                .addRoundTrip("Map(Int32, Float64)", "map(42, 8)", mapWithValueType(DOUBLE), "MAP(ARRAY[42], ARRAY[DOUBLE '8'])")
                .addRoundTrip("Map(Int32, FixedString(4))", "map(42, 'nine')", mapWithValueType(VARCHAR), "MAP(ARRAY[42], ARRAY[CAST('nine' AS VARCHAR)])")
                .addRoundTrip("Map(Int32, String)", "map(42, 'ten')", mapWithValueType(VARCHAR), "MAP(ARRAY[42], ARRAY[CAST('ten' AS VARCHAR)])")
                .addRoundTrip("Map(Int32, Date)", "map(42, '2011-11-11')", mapWithValueType(DATE), "MAP(ARRAY[42], ARRAY[DATE '2011-11-11'])")
                .addRoundTrip("Map(Int32, Enum('hello' = 1, 'world' = 2))", "map(42, 'world')", mapWithValueType(VARCHAR), "MAP(ARRAY[42], ARRAY[CAST('world' AS VARCHAR)])")
                .addRoundTrip("Map(Int32, UUID)", "map(42, '92d3f742-b13c-4d8e-9d7a-1130d2d31980')", mapWithValueType(UUID), "MAP(ARRAY[42], ARRAY[UUID '92d3f742-b13c-4d8e-9d7a-1130d2d31980'])");
        return dataTypeTest;
    }}
