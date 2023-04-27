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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestDeltaLakePartitioning
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.register-table-procedure.enabled", "true"));
    }

    @BeforeClass
    public void registerTables()
    {
        String dataPath = getClass().getClassLoader().getResource("deltalake/partitions").toExternalForm();
        getQueryRunner().execute(format("CALL system.register_table('%s', 'partitions', '%s')", getSession().getSchema().orElseThrow(), dataPath));
    }

    @Test
    public void testDescribeTable()
    {
        // the schema is actually defined in the transaction log
        assertQuery(
                "DESCRIBE partitions",
                "VALUES " +
                        "('p_string', 'varchar', '', ''), " +
                        "('p_byte', 'tinyint', '', ''), " +
                        "('p_short', 'smallint', '', ''), " +
                        "('p_int', 'integer', '', ''), " +
                        "('p_long', 'bigint', '', ''), " +
                        "('p_decimal', 'decimal(38,18)', '', ''), " +
                        "('p_boolean', 'boolean', '', ''), " +
                        "('p_float', 'real', '', ''), " +
                        "('p_double', 'double', '', ''), " +
                        "('p_date', 'date', '', ''), " +
                        "('p_timestamp', 'timestamp(3) with time zone', '', ''), " +
                        "('t_string', 'varchar', '', ''), " +
                        "('t_byte', 'tinyint', '', ''), " +
                        "('t_short', 'smallint', '', ''), " +
                        "('t_int', 'integer', '', ''), " +
                        "('t_long', 'bigint', '', ''), " +
                        "('t_decimal', 'decimal(38,18)', '', ''), " +
                        "('t_boolean', 'boolean', '', ''), " +
                        "('t_float', 'real', '', ''), " +
                        "('t_double', 'double', '', ''), " +
                        "('t_date', 'date', '', ''), " +
                        "('t_timestamp', 'timestamp(3) with time zone', '', ''), " +
                        "('t_phones', 'array(row(number varchar, label varchar))', '', ''), " +
                        "('t_address', 'row(street varchar, city varchar, state varchar, zip varchar)', '', '')");
    }

    @Test
    public void testReadAllTypes()
    {
        assertQuery(
                "SELECT " +
                        "p_string, " +
                        "p_byte, " +
                        "p_short, " +
                        "p_int, " +
                        "p_long, " +
                        "p_decimal, " +
                        "p_boolean, " +
                        "p_float, " +
                        "p_double, " +
                        "p_date, " +
                        // H2QueryRunner does not support TIMESTAMP WITH TIME ZONE,
                        // so instead we convert the TIMESTAMP WITH TIME ZONE from Delta to a string representation and compare that.
                        "CAST(p_timestamp AS VARCHAR), " +
                        "t_string, " +
                        "t_byte, " +
                        "t_short, " +
                        "t_int, " +
                        "t_long, " +
                        "t_decimal, " +
                        "t_boolean, " +
                        "t_float, " +
                        "t_double, " +
                        "t_date, " +
                        "CAST(t_timestamp AS VARCHAR), " +
                        "t_phones[1].number, " +
                        "t_address.street " +
                        "FROM partitions " +
                        "ORDER BY t_int " +
                        "LIMIT 1 ",
                "VALUES (" +
                        "'Alice', " +
                        "123, " +
                        "12345, " +
                        "123456789, " +
                        "1234567890123456789, " +
                        "12345678901234567890.123456789012345678, " +
                        "true, " +
                        "3.1415927, " +
                        "3.141592653589793, " +
                        "DATE '2014-01-01', " +
                        "'2014-01-01 23:00:01.123 UTC', " +
                        "'Bob', " +
                        "-77, " +
                        "23456, " +
                        "1, " +
                        "2345678901234567890, " +
                        "23456789012345678901.234567890123456789, " +
                        "false, " +
                        "2.7182817, " +
                        "2.718281828459045, " +
                        "DATE '2020-01-01', " +
                        "'2020-01-01 23:00:01.123 UTC', " +
                        "'123-555-0000', " +
                        "'100 Main St'" +
                        ")");
    }

    @Test
    public void testPartitionsSystemTableDoesNotExist()
    {
        assertQueryFails(
                "SELECT * FROM \"partitions$partitions\"",
                ".*'delta\\.tpch\\.partitions\\$partitions' does not exist");
    }

    @Test
    public void testPartitioningWithSpecialCharactersInPartitionColumn()
    {
        String dataPath = getClass().getClassLoader().getResource("deltalake").toExternalForm() + "/special_char" + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE special_chars (id, col_name) " +
                        "WITH(partitioned_by = ARRAY['col_name'], " +
                        "location = '" + dataPath + "') " +
                        "AS VALUES" +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')", 9);

        assertQuery("SELECT * FROM special_chars", "VALUES " +
                "(1, 'with-hyphen'), " +
                "(2, 'with.dot'), " +
                "(3, 'with:colon')," +
                "(4, 'with/slash'), " +
                "(5, 'with\\\\backslash'), " +
                "(6, 'with=equal'), " +
                "(7, 'with?question'), " +
                "(8, 'with!exclamation'), " +
                "(9, 'with%%percent')");
    }

    @Test
    public void testRevealPartitionColumnInExplain()
    {
        assertExplain("EXPLAIN SELECT * FROM partitions", "p_string := p_string:varchar:PARTITION_KEY");
    }
}
