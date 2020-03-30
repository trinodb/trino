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
package com.starburstdata.presto.plugin.snowflake;

import io.prestosql.spi.type.VarcharType;
import io.prestosql.tests.datatype.DataTypeTest;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.tests.datatype.DataType.stringDataType;
import static io.prestosql.tests.datatype.DataType.varcharDataType;
import static java.lang.String.format;

public class TestDistributedSnowflakeTypeMapping
        extends BaseSnowflakeTypeMappingTest
{
    public TestDistributedSnowflakeTypeMapping()
    {
        this(new SnowflakeServer());
    }

    private TestDistributedSnowflakeTypeMapping(SnowflakeServer server)
    {
        super(server, () -> distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .build());
    }

    @Test
    public void varcharMapping()
    {
        testTypeMapping(
                DataTypeTest.create()
                        .addRoundTrip(varcharDataType(10), "string 010")
                        .addRoundTrip(varcharDataType(20), "string 020")
                        .addRoundTrip(stringDataType("varchar(" + MAX_VARCHAR + ")", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "string max size")
                        .addRoundTrip(varcharDataType(5), null)
                        .addRoundTrip(varcharDataType(213), "攻殻機動隊")
                        .addRoundTrip(varcharDataType(42), null));
    }

    @Test
    public void varcharReadMapping()
    {
        testTypeReadMapping(
                DataTypeTest.create()
                        .addRoundTrip(stringDataType("varchar(10)", VarcharType.createVarcharType(10)), "string 010")
                        .addRoundTrip(stringDataType("varchar(20)", VarcharType.createVarcharType(20)), "string 020")
                        .addRoundTrip(stringDataType(format("varchar(%s)", MAX_VARCHAR), VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "string max size")
                        .addRoundTrip(stringDataType("character(10)", VarcharType.createVarcharType(10)), null)
                        .addRoundTrip(stringDataType("char(100)", VarcharType.createVarcharType(100)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("text", VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("string", VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "攻殻機動隊"));
    }
}
