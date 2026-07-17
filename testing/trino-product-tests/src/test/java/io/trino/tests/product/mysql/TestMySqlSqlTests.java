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
package io.trino.tests.product.mysql;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MySQL connector SQL operations.
 * <p>
 * Migrated from Tempto SQL tests to JUnit 5 with TestContainers.
 */
@ProductTest
@RequiresEnvironment(MySqlEnvironment.class)
@TestGroup.Mysql
@TestGroup.ProfileSpecificTests
class TestMySqlSqlTests
{
    // Expected rows for workers_mysql table
    // Schema: id_employee (integer), first_name (varchar), last_name (varchar),
    //         date_of_employment (date), department (tinyint), id_department (integer),
    //         name (varchar), salary (integer)
    private static final List<Row> WORKERS_MYSQL_ROWS = List.of(
            row(null, null, null, null, null, 1, "Marketing", 4000),
            row(2, "Ann", "Turner", Date.valueOf("2000-05-28"), (short) 2, 2, "R&D", 5000),
            row(3, "Martin", "Smith", Date.valueOf("2000-05-28"), (short) 2, 2, "R&D", 5000),
            row(null, null, null, null, null, 3, "Finance", 3000),
            row(4, "Joana", "Donne", Date.valueOf("2002-04-05"), (short) 4, 4, "IT", 4000),
            row(5, "Kate", "Grant", Date.valueOf("2001-04-06"), (short) 5, 5, "HR", 2000),
            row(6, "Christopher", "Johnson", Date.valueOf("2001-04-06"), (short) 5, 5, "HR", 2000),
            row(null, null, null, null, null, 6, "PR", 3000),
            row(7, "George", "Cage", Date.valueOf("2003-10-09"), (short) 7, 7, "CustomerService", 2300),
            row(8, "Jacob", "Brown", Date.valueOf("2003-10-09"), (short) 8, 8, "Production", 2400),
            row(9, "John", "Black", Date.valueOf("2004-05-09"), (short) 9, 9, "Quality", 3400),
            row(null, null, null, null, null, 10, "Sales", 3500),
            row(10, "Charlie", "Page", Date.valueOf("2000-11-12"), (short) 11, null, null, null),
            row(1, "Mary", "Parker", Date.valueOf("1999-04-03"), (short) 12, null, null, null));

    // Expected rows for real_table_mysql table
    // Schema: id_employee (integer), salary (double), bonus (real), tip (real), tip2 (double)
    // Note: MySQL FLOAT maps to Trino real, FLOAT(30) maps to double
    private static final List<Row> REAL_TABLE_MYSQL_ROWS = List.of(
            row(null, 4000.10889, 217.646f, 348.654f, 50.49),
            row(2, 100.97, 0.8104f, 0.438f, 0.58),
            row(null, null, null, null, null));

    @Test
    void testSelect(MySqlEnvironment env)
    {
        QueryResult result = env.executeTrino("SELECT * FROM mysql.test.workers_mysql");
        assertThat(result).containsOnly(WORKERS_MYSQL_ROWS);
    }

    @Test
    void testDescribeTable(MySqlEnvironment env)
    {
        assertThat(env.executeTrino("DESCRIBE mysql.test.workers_mysql"))
                .containsOnly(List.of(
                        row("id_employee", "integer", "", ""),
                        row("first_name", "varchar(32)", "", ""),
                        row("last_name", "varchar(32)", "", ""),
                        row("date_of_employment", "date", "", ""),
                        row("department", "tinyint", "", ""),
                        row("id_department", "integer", "", ""),
                        row("name", "varchar(32)", "", ""),
                        row("salary", "integer", "", "")));
    }

    @Test
    void testDescribeRealTable(MySqlEnvironment env)
    {
        assertThat(env.executeTrino("DESCRIBE mysql.test.real_table_mysql"))
                .containsOnly(List.of(
                        row("id_employee", "integer", "", ""),
                        row("salary", "double", "", ""),
                        row("bonus", "real", "", ""),
                        row("tip", "real", "", ""),
                        row("tip2", "double", "", "")));
    }

    @Test
    void testJoinMysqlToMysql(MySqlEnvironment env)
    {
        assertThat(env.executeTrino(
                """
                SELECT t1.last_name, t2.first_name
                FROM mysql.test.workers_mysql t1, mysql.test.workers_mysql t2
                WHERE t1.id_department = t2.id_employee
                """))
                .containsOnly(List.of(
                        row(null, "Mary"),
                        row("Turner", "Ann"),
                        row("Smith", "Ann"),
                        row(null, "Martin"),
                        row("Donne", "Joana"),
                        row("Grant", "Kate"),
                        row("Johnson", "Kate"),
                        row(null, "Christopher"),
                        row("Cage", "George"),
                        row("Brown", "Jacob"),
                        row("Black", "John"),
                        row(null, "Charlie")));
    }

    @Test
    void testJoinMysqlToTpch(MySqlEnvironment env)
    {
        assertThat(env.executeTrino(
                """
                SELECT t1.first_name, t2.name
                FROM mysql.test.workers_mysql t1, tpch.sf1.nation t2
                WHERE t1.id_department = t2.nationkey
                """))
                .containsOnly(List.of(
                        row(null, "ARGENTINA"),
                        row("Ann", "BRAZIL"),
                        row("Martin", "BRAZIL"),
                        row(null, "CANADA"),
                        row("Joana", "EGYPT"),
                        row("Kate", "ETHIOPIA"),
                        row("Christopher", "ETHIOPIA"),
                        row(null, "FRANCE"),
                        row("George", "GERMANY"),
                        row("Jacob", "INDIA"),
                        row("John", "INDONESIA"),
                        row(null, "IRAN")));
    }

    @Test
    void testSelectReal(MySqlEnvironment env)
    {
        QueryResult result = env.executeTrino("SELECT * FROM mysql.test.real_table_mysql");
        assertThat(result).containsOnly(REAL_TABLE_MYSQL_ROWS);
    }

    @Test
    void testShowSchemas(MySqlEnvironment env)
    {
        QueryResult result = env.executeTrino("SHOW SCHEMAS FROM mysql");
        List<String> schemas = result.column(1).stream()
                .map(Object::toString)
                .toList();
        assertThat(schemas).contains("test", "information_schema");
    }

    @Test
    void testShowTables(MySqlEnvironment env)
    {
        QueryResult result = env.executeTrino("SHOW TABLES FROM mysql.test");
        List<String> tables = result.column(1).stream()
                .map(Object::toString)
                .toList();
        assertThat(tables).containsExactlyInAnyOrder(
                "datatype_mysql",
                "workers_mysql",
                "real_table_mysql");
    }

    @Test
    void testTinyintFilter(MySqlEnvironment env)
    {
        assertThat(env.executeTrino("SELECT * FROM mysql.test.workers_mysql WHERE department = 2"))
                .containsOnly(List.of(
                        row(2, "Ann", "Turner", Date.valueOf("2000-05-28"), (short) 2, 2, "R&D", 5000),
                        row(3, "Martin", "Smith", Date.valueOf("2000-05-28"), (short) 2, 2, "R&D", 5000)));
    }
}
