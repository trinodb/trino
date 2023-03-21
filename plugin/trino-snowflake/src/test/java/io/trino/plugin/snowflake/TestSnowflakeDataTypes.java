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
package io.trino.plugin.snowflake;

import io.trino.spi.type.BigintType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.testng.annotations.Test;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestSnowflakeDataTypes
        extends AbstractTestBase
{
    private DistributedQueryRunner queryRunner;

    @Test
    public void testDoubleTypes()
            throws Exception
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "123.45", DOUBLE, "DOUBLE '123.45'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("doubles"));
    }

    @Test
    public void testNumericTypes()
            throws Exception
    {
        SqlDataTypeTest.create()
                .addRoundTrip("INT", "123", BigintType.BIGINT, "BIGINT '123'")
                .addRoundTrip("BIGINT", "999999999", BigintType.BIGINT, "BIGINT '999999999'")
                .addRoundTrip("SMALLINT", "123", BigintType.BIGINT, "BIGINT '123'")
                .addRoundTrip("TINYINT", "123", BigintType.BIGINT, "BIGINT '123'")
                .execute(getQueryRunner(), snowflakeCreateAndInsert("numerics"));
    }

    private DataSetup snowflakeCreateAndInsert(String tableNamePrefix)
    {
        tableNamePrefix = "public." + tableNamePrefix;
        System.out.println(tableNamePrefix);
        return new CreateAndInsertDataSetup(new JdbcSqlExecutor(SnowflakeSqlQueryRunner.getJdbcUrl(), SnowflakeSqlQueryRunner.getProperties()), tableNamePrefix);
    }
}
