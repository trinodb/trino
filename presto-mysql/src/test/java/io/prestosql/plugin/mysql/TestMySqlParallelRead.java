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
package io.prestosql.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestMySqlParallelRead
        extends AbstractTestQueryFramework
{
    private final TestingMySqlServer mySqlServer;

    public TestMySqlParallelRead() throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass"));
    }

    public TestMySqlParallelRead(TestingMySqlServer mySqlServer) throws Exception
    {
        super(() -> MySqlQueryRunner.createMySqlQueryRunner(
                mySqlServer,
                ImmutableMap.of("parallel-read-enabled", "true"),
                ImmutableList.of()));
        this.mySqlServer = mySqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mySqlServer.close();
    }

    @Test
    public void testQueryRangePartitionedTable()
            throws Exception
    {
        try (AutoCloseable ignore1 = prepareRangePartitionedTable()) {
            MaterializedResult actualColumns = computeActual("select * from partitioned_db.partitioned_tbl").toTestTypes();

            MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, VarcharType.createVarcharType(100), INTEGER)
                    .row(1L, "james", 1)
                    .row(4L, "bond", 4)
                    .row(8L, "lily", 8)
                    .row(10L, "lucy", 10)
                    .build();
            assertEquals(actualColumns, expectedColumns);
        }
    }

    @Test
    public void testQueryNormalTable()
            throws Exception
    {
        try (AutoCloseable ignore1 = prepareNormalTable()) {
            MaterializedResult actualColumns = computeActual("select * from normal_db.normal_tbl").toTestTypes();

            MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, VarcharType.createVarcharType(100), INTEGER)
                    .row(1L, "james", 1)
                    .row(4L, "bond", 4)
                    .build();
            assertEquals(actualColumns, expectedColumns);
        }
    }

    private AutoCloseable prepareRangePartitionedTable()
    {
        execute("create database if not exists partitioned_db;");
        execute(format("create table partitioned_db.partitioned_tbl (\n" +
                "    id bigint,\n" +
                "    name varchar(100),\n" +
                "    age int\n" +
                ")     \n" +
                "PARTITION BY RANGE(id)\n" +
                "(\n" +
                "    PARTITION p0 VALUES LESS THAN (3),\n" +
                "    PARTITION p1 VALUES LESS THAN (7),\n" +
                "    PARTITION p2 VALUES LESS THAN (9),\n" +
                "    PARTITION p3 VALUES LESS THAN (11)\n" +
                ");"));
        execute("INSERT INTO partitioned_db.partitioned_tbl values (1, 'james', 1)");
        execute("INSERT INTO partitioned_db.partitioned_tbl values (4, 'bond', 4)");
        execute("INSERT INTO partitioned_db.partitioned_tbl values (8, 'lily', 8)");
        execute("INSERT INTO partitioned_db.partitioned_tbl values (10, 'lucy', 10)");

        return () -> execute(format("DROP DATABASE partitioned_db"));
    }

    private AutoCloseable prepareNormalTable()
    {
        execute("create database if not exists normal_db;");
        execute(format("create table normal_db.normal_tbl (\n" +
                "    id bigint,\n" +
                "    name varchar(100),\n" +
                "    age int\n" +
                ");"));
        execute("INSERT INTO normal_db.normal_tbl values (1, 'james', 1)");
        execute("INSERT INTO normal_db.normal_tbl values (4, 'bond', 4)");

        return () -> execute(format("DROP DATABASE normal_db"));
    }

    private void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(mySqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }
}
