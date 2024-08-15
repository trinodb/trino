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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeSystemTables
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .build();
    }

    @Test
    public void testHistoryTable()
    {
        try {
            assertUpdate("CREATE TABLE test_simple_table (_bigint BIGINT)");
            assertUpdate("INSERT INTO test_simple_table VALUES 1, 2, 3", 3);
            assertQuery("SELECT count(*) FROM test_simple_table", "VALUES 3");

            assertUpdate("CREATE TABLE test_checkpoint_table (_bigint BIGINT, _date DATE) WITH (partitioned_by = ARRAY['_date'] )");
            assertUpdate("INSERT INTO test_checkpoint_table VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
            assertUpdate("INSERT INTO test_checkpoint_table VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
            assertUpdate("UPDATE test_checkpoint_table SET _bigint = 50 WHERE _bigint =  BIGINT '5'", 1);
            assertUpdate("DELETE FROM test_checkpoint_table WHERE _date =  DATE '2019-09-08'", 1);
            assertQuerySucceeds("ALTER TABLE test_checkpoint_table EXECUTE OPTIMIZE");
            assertQuery("SELECT count(*) FROM test_checkpoint_table", "VALUES 5");

            assertQuery("SHOW COLUMNS FROM \"test_checkpoint_table$history\"",
                    """
                            VALUES
                            ('version', 'bigint', '', ''),
                            ('timestamp', 'timestamp(3) with time zone', '', ''),
                            ('user_id', 'varchar', '', ''),
                            ('user_name', 'varchar', '', ''),
                            ('operation', 'varchar', '', ''),
                            ('operation_parameters', 'map(varchar, varchar)', '', ''),
                            ('cluster_id', 'varchar', '', ''),
                            ('read_version', 'bigint', '', ''),
                            ('isolation_level', 'varchar', '', ''),
                            ('is_blind_append', 'boolean', '', '')
                            """);

            // Test the contents of history system table
            assertThat(query("SELECT version, operation, read_version, isolation_level, is_blind_append FROM \"test_simple_table$history\""))
                    .matches("""
                            VALUES
                                (BIGINT '1', VARCHAR 'WRITE', BIGINT '0', VARCHAR 'WriteSerializable', true),
                                (BIGINT '0', VARCHAR 'CREATE TABLE', BIGINT '0', VARCHAR 'WriteSerializable', true)
                            """);
            assertThat(query("SELECT version, operation, read_version, isolation_level, is_blind_append FROM \"test_checkpoint_table$history\""))
                    // TODO (https://github.com/trinodb/trino/issues/15763) Use correct operation name for DML statements
                    .matches("""
                            VALUES
                                (BIGINT '5', VARCHAR 'OPTIMIZE', BIGINT '4', VARCHAR 'WriteSerializable', false),
                                (BIGINT '4', VARCHAR 'DELETE', BIGINT '3', VARCHAR 'WriteSerializable', false),
                                (BIGINT '3', VARCHAR 'MERGE', BIGINT '2', VARCHAR 'WriteSerializable', false),
                                (BIGINT '2', VARCHAR 'WRITE', BIGINT '1', VARCHAR 'WriteSerializable', true),
                                (BIGINT '1', VARCHAR 'WRITE', BIGINT '0', VARCHAR 'WriteSerializable', true),
                                (BIGINT '0', VARCHAR 'CREATE TABLE', BIGINT '0', VARCHAR 'WriteSerializable', true)
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_simple_table");
            assertUpdate("DROP TABLE IF EXISTS test_checkpoint_table");
        }
    }

    @Test
    public void testPropertiesTable()
    {
        String tableName = "test_simple_properties_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (_bigint BIGINT) WITH (change_data_feed_enabled = true, checkpoint_interval = 5)");
            assertQuery("SELECT * FROM \"" + tableName + "$properties\"", "VALUES " +
                    "('delta.enableChangeDataFeed', 'true')," +
                    "('delta.enableDeletionVectors', 'false')," +
                    "('delta.checkpointInterval', '5')," +
                    "('delta.minReaderVersion', '1')," +
                    "('delta.minWriterVersion', '4')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionsTable()
    {
        String tableName = "test_simple_partitions_table_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _date DATE) WITH (partitioned_by = ARRAY['_date'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, NULL)", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 7");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 4");

            assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('partition', 'row(_date date)', '', ''),
                            ('file_count', 'bigint', '', ''),
                            ('total_size', 'bigint', '', ''),
                            ('data', 'row(_bigint row(min bigint, max bigint, null_count bigint))', '', '')
                            """);

            assertQuery("SELECT partition._date FROM \"" + tableName + "$partitions\"", " VALUES DATE '2019-09-08', DATE '2019-09-09', DATE '2019-09-10', NULL");

            assertThat(query("SELECT CAST(data._bigint AS ROW(BIGINT, BIGINT, BIGINT)) FROM \"" + tableName + "$partitions\""))
                    .matches("""
                            VALUES
                            ROW(ROW(BIGINT '0', BIGINT '0', BIGINT '0')),
                            ROW(ROW(BIGINT '1', BIGINT '3', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '5', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '6', BIGINT '0'))
                            """);

            assertUpdate("INSERT INTO " + tableName + " VALUES (NULL, CAST('2019-09-09' AS DATE))", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 8");
            assertThat(query("SELECT CAST(data._bigint AS ROW(BIGINT, BIGINT, BIGINT)) FROM \"" + tableName + "$partitions\""))
                    .matches("""
                            VALUES
                            ROW(ROW(BIGINT '0', BIGINT '0', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '5', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '6', BIGINT '0')),
                            ROW(ROW(NULL, NULL, BIGINT '1'))
                            """);
        }
        finally{
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * @see databricks133.partition_values_parsed_case_sensitive
     */
    @Test
    public void testPartitionsTableCaseSensitiveColumns()
            throws Exception
    {
        String tableName = "test_partitions_table_case_sensitive_columns_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("databricks133/partition_values_parsed_case_sensitive").toURI()).toPath(), tableLocation);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));

        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
        assertQuery("SELECT * FROM " + tableName, "VALUES (100, 1, 'ala'), (200, 2, 'kota'), (300, 3, 'osla')");

        assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 3");

        assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                """
                        VALUES
                        ('partition', 'row(part_NuMbEr integer, part_StRiNg varchar)', '', ''),
                        ('file_count', 'bigint', '', ''),
                        ('total_size', 'bigint', '', ''),
                        ('data', 'row(id row(min integer, max integer, null_count bigint))', '', '')
                        """);

        assertQuery("SELECT partition.part_NuMbEr, partition.part_StRiNg FROM \"" + tableName + "$partitions\"", "VALUES (1, 'ala'), (2, 'kota'), (3, 'osla')");

        assertThat(query("SELECT CAST(data.id AS ROW(INTEGER, INTEGER, BIGINT)) FROM \"" + tableName + "$partitions\""))
                .matches("VALUES ROW(ROW(100, 100, BIGINT '0')), ROW(ROW(200, 200, BIGINT '0')), ROW(ROW(300, 300, BIGINT '0'))");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1, 'ala'), (2, 2, 'kota'), (3, 3, 'osla')", 3);
        assertThat(query("SELECT CAST(data.id AS ROW(INTEGER, INTEGER, BIGINT)) FROM \"" + tableName + "$partitions\""))
                .matches("VALUES ROW(ROW(1, 100, BIGINT '0')), ROW(ROW(2, 200, BIGINT '0')), ROW(ROW(3, 300, BIGINT '0'))");
    }

    @Test
    public void testColumnMappingModePartitionsTable()
    {
        for (String columnMappingMode : ImmutableList.of("id", "name", "none")) {
            testColumnMappingModePartitionsTable(columnMappingMode);
        }
    }

    private void testColumnMappingModePartitionsTable(String columnMappingMode)
    {
        String tableName = "test_simple_column_mapping_mode_" + columnMappingMode + "_partitions_table_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _date DATE) WITH (column_mapping_mode = '" + columnMappingMode + "', partitioned_by = ARRAY['_date'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, NULL), (NULL, CAST('2019-09-08' AS DATE)), (NULL, CAST('2019-09-08' AS DATE))", 3);

            assertQuerySucceeds("SELECT * FROM " + tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 9");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 4");

            assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('partition', 'row(_date date)', '', ''),
                            ('file_count', 'bigint', '', ''),
                            ('total_size', 'bigint', '', ''),
                            ('data', 'row(_bigint row(min bigint, max bigint, null_count bigint))', '', '')
                            """);

            assertQuery("SELECT partition._date FROM \"" + tableName + "$partitions\"", " VALUES DATE '2019-09-08', DATE '2019-09-09', DATE '2019-09-10', NULL");

            assertThat(query("SELECT CAST(data._bigint AS ROW(BIGINT, BIGINT, BIGINT)) FROM \"" + tableName + "$partitions\""))
                    .matches("""
                            VALUES
                            ROW(ROW(BIGINT '1', BIGINT '3', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '5', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '6', BIGINT '0')),
                            ROW(ROW(CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), BIGINT '2'))
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionsTableMultipleColumns()
    {
        String tableName = "test_partitions_table_multiple_columns_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _date DATE, _varchar VARCHAR) WITH (partitioned_by = ARRAY['_date', '_varchar'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, CAST('2019-09-08' AS DATE), 'a'), (1, CAST('2019-09-09' AS DATE), 'b'), (2, CAST('2019-09-09' AS DATE), 'c')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, CAST('2019-09-09' AS DATE), 'd'), (4, CAST('2019-09-10' AS DATE), 'e'), (5, CAST('2019-09-10' AS DATE), 'f'), (4, CAST('2019-09-10' AS DATE), 'f')", 4);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, null, 'g'), (6, CAST('2019-09-10' AS DATE), null), (7, null, null), (8, null, 'g')", 4);
            assertUpdate("UPDATE " + tableName + " SET _bigint = 50 WHERE _bigint =  BIGINT '5'", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE _date =  DATE '2019-09-08'", 1);
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 10");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 8");
            assertQuery("SELECT count(partition._varchar) FROM \"" + tableName + "$partitions\"", "VALUES 6");
            assertQuery("SELECT count(distinct partition._date) FROM \"" + tableName + "$partitions\"", "VALUES 2");

            assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('partition', 'row(_date date, _varchar varchar)', '', ''),
                            ('file_count', 'bigint', '', ''),
                            ('total_size', 'bigint', '', ''),
                            ('data', 'row(_bigint row(min bigint, max bigint, null_count bigint))', '', '')
                            """);

            assertQuery(
                    "SELECT partition._date, partition._varchar FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            (DATE '2019-09-09', 'b'),
                            (DATE '2019-09-09', 'c'),
                            (DATE '2019-09-09', 'd'),
                            (DATE '2019-09-10', 'e'),
                            (DATE '2019-09-10', 'f'),
                            (DATE '2019-09-10', null),
                            (null, 'g'),
                            (null, null)
                            """);

            assertThat(query("SELECT CAST(data._bigint AS ROW(BIGINT, BIGINT, BIGINT)) FROM \"" + tableName + "$partitions\""))
                    .matches("""
                            VALUES
                            ROW(ROW(BIGINT '1', BIGINT '1', BIGINT '0')),
                            ROW(ROW(BIGINT '2', BIGINT '2', BIGINT '0')),
                            ROW(ROW(BIGINT '3', BIGINT '3', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '4', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '50', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '6', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '8', BIGINT '0')),
                            ROW(ROW(BIGINT '7', BIGINT '7', BIGINT '0'))
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionsTableDifferentOrderFromDefinitionMultipleColumns()
    {
        String tableName = "test_partitions_table_different_order_from_definition_multiple_columns_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _date DATE, _varchar VARCHAR) WITH (partitioned_by = ARRAY['_varchar', '_date'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, CAST('2019-09-08' AS DATE), 'a'), (1, CAST('2019-09-09' AS DATE), 'b'), (2, CAST('2019-09-09' AS DATE), 'c')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, CAST('2019-09-09' AS DATE), 'd'), (4, CAST('2019-09-10' AS DATE), 'e'), (5, CAST('2019-09-10' AS DATE), 'f'), (4, CAST('2019-09-10' AS DATE), 'f')", 4);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, null, 'g'), (6, CAST('2019-09-10' AS DATE), null), (7, null, null), (8, null, 'g')", 4);
            assertUpdate("UPDATE " + tableName + " SET _bigint = 50 WHERE _bigint =  BIGINT '5'", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE _date =  DATE '2019-09-08'", 1);
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 10");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 8");
            assertQuery("SELECT count(partition._varchar) FROM \"" + tableName + "$partitions\"", "VALUES 6");
            assertQuery("SELECT count(distinct partition._date) FROM \"" + tableName + "$partitions\"", "VALUES 2");

            assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('partition', 'row(_varchar varchar, _date date)', '', ''),
                            ('file_count', 'bigint', '', ''),
                            ('total_size', 'bigint', '', ''),
                            ('data', 'row(_bigint row(min bigint, max bigint, null_count bigint))', '', '')
                            """);

            assertQuery(
                    "SELECT partition._varchar, partition._date FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('b', DATE '2019-09-09'),
                            ('c', DATE '2019-09-09'),
                            ('d', DATE '2019-09-09'),
                            ('e', DATE '2019-09-10'),
                            ('f', DATE '2019-09-10'),
                            (null, DATE '2019-09-10'),
                            ('g', null),
                            (null, null)
                            """);

            assertThat(query("SELECT CAST(data._bigint AS ROW(BIGINT, BIGINT, BIGINT)) FROM \"" + tableName + "$partitions\""))
                    .matches("""
                            VALUES
                            ROW(ROW(BIGINT '1', BIGINT '1', BIGINT '0')),
                            ROW(ROW(BIGINT '2', BIGINT '2', BIGINT '0')),
                            ROW(ROW(BIGINT '3', BIGINT '3', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '4', BIGINT '0')),
                            ROW(ROW(BIGINT '4', BIGINT '50', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '6', BIGINT '0')),
                            ROW(ROW(BIGINT '6', BIGINT '8', BIGINT '0')),
                            ROW(ROW(BIGINT '7', BIGINT '7', BIGINT '0'))
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionsTableColumnTypes()
    {
        testPartitionsTableColumnTypes("BOOLEAN", "VALUES (true, 'a'), (false, 'a'), (false, 'b'), (false, 'b')", 4, """
                VALUES
                ROW(ROW(CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), BIGINT '0')),
                ROW(ROW(CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("INTEGER", "VALUES (3, 'a'), (6, 'a'), (0, 'b'), (9, 'b')", 4, """
                VALUES
                ROW(ROW(0, 9, BIGINT '0')),
                ROW(ROW(3, 6, BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("TINYINT", "VALUES (3, 'a'), (6, 'a'), (0, 'b'), (9, 'b')", 4, """
                VALUES
                ROW(ROW(TINYINT '0', TINYINT '9', BIGINT '0')),
                ROW(ROW(TINYINT '3', TINYINT '6', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("SMALLINT", "VALUES (3, 'a'), (6, 'a'), (0, 'b'), (9, 'b')", 4, """
                VALUES
                ROW(ROW(SMALLINT '0', SMALLINT '9', BIGINT '0')),
                ROW(ROW(SMALLINT '3', SMALLINT '6', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("BIGINT", "VALUES (3, 'a'), (6, 'a'), (0, 'b'), (9, 'b')", 4, """
                VALUES
                ROW(ROW(BIGINT '0', BIGINT '9', BIGINT '0')),
                ROW(ROW(BIGINT '3', BIGINT '6', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("REAL", "VALUES (10.3, 'a'), (15.7, 'a'), (3.2, 'b'), (6.1, 'b')", 4, """
                VALUES
                ROW(ROW(REAL '3.2', REAL '6.1', BIGINT '0')),
                ROW(ROW(REAL '10.3', REAL '15.7', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("DOUBLE", "VALUES (3.2, 'a'), (7.25, 'a'), (7.25, 'b'), (18.9382, 'b')", 4, """
                VALUES
                ROW(ROW(DOUBLE '3.2', DOUBLE '7.25', BIGINT '0')),
                ROW(ROW(DOUBLE '7.25', DOUBLE '18.9382', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("DECIMAL(10)", "VALUES (5.6, 'a'), (1.2, 'a'), (532.62, 'b'), (153.27, 'b')", 4, """
                VALUES
                ROW(ROW(CAST(1.2 AS DECIMAL(10)), CAST(5.6 AS DECIMAL(10)), BIGINT '0')),
                ROW(ROW(CAST(153.27 AS DECIMAL(10)), CAST(532.62 AS DECIMAL(10)), BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("DECIMAL(20)", "VALUES (0.64525495002404036507, 'a'), (0.77003757467454995626, 'a'), (0.05016312397354421814, 'b'), (0.69575427222174470843, 'b')", 4, """
                VALUES
                ROW(ROW(CAST(0.05016312397354421814 AS DECIMAL(20)), CAST(0.69575427222174470843 AS DECIMAL(20)), BIGINT '0')),
                ROW(ROW(CAST(0.64525495002404036507 AS DECIMAL(20)), CAST(0.77003757467454995626 AS DECIMAL(20)), BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("DATE", "VALUES (CAST('2019-09-08' AS DATE), 'a'), (CAST('2020-09-08' AS DATE), 'a'), (CAST('2019-09-07' AS DATE), 'b'), (CAST('2019-09-08' AS DATE), 'b')", 4, """
                VALUES
                ROW(ROW(DATE '2019-09-07', DATE '2019-09-08', BIGINT '0')),
                ROW(ROW(DATE '2019-09-08', DATE '2020-09-08', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("TIMESTAMP(6)", "VALUES (TIMESTAMP '2001-05-06 12:34:56.123456', 'a'), (TIMESTAMP '2001-05-06 12:34:56.567890', 'a'), (TIMESTAMP '2001-05-06 12:34:56.123456', 'b'), (TIMESTAMP '2001-05-06 12:34:56.123457', 'b')", 4, """
                VALUES
                ROW(ROW(TIMESTAMP '2001-05-06 12:34:56.123000', TIMESTAMP '2001-05-06 12:34:56.568000', BIGINT '0')),
                ROW(ROW(TIMESTAMP '2001-05-06 12:34:56.123000', TIMESTAMP '2001-05-06 12:34:56.124000', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("TIMESTAMP(3) WITH TIME ZONE", "VALUES (TIMESTAMP '2001-05-06 12:34:56.123 UTC', 'a'), (TIMESTAMP '2001-05-06 12:34:56.234 -08:30', 'a'), (TIMESTAMP '2001-05-06 12:34:56.567 GMT-08:30', 'b'), (TIMESTAMP '2001-05-06 12:34:56.789 America/New_York', 'b')", 4, """
                VALUES
                ROW(ROW(TIMESTAMP '2001-05-06 12:34:56.123 UTC', TIMESTAMP '2001-05-06 21:04:56.234 UTC', BIGINT '0')),
                ROW(ROW(TIMESTAMP '2001-05-06 16:34:56.789 UTC', TIMESTAMP '2001-05-06 21:04:56.567 UTC', BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("VARCHAR", "VALUES ('z', 'a'), ('x', 'a'), ('a', 'b'), ('b', 'b')", 4, """
                VALUES
                ROW(ROW(CAST('a' AS VARCHAR), CAST('b' AS VARCHAR), BIGINT '0')),
                ROW(ROW(CAST('x' AS VARCHAR), CAST('z' AS VARCHAR), BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("VARBINARY", "VALUES (VARBINARY 'abcd', 'a'), (VARBINARY 'jkl', 'a'), (VARBINARY 'mno', 'b'), (VARBINARY 'xyzz', 'b')", 4, """
                VALUES
                ROW(ROW(CAST(NULL AS VARBINARY), CAST(NULL AS VARBINARY), BIGINT '0')),
                ROW(ROW(CAST(NULL AS VARBINARY), CAST(NULL AS VARBINARY), BIGINT '0'))
                """);
        testPartitionsTableColumnTypes("ARRAY(INTEGER)", "VALUES (ARRAY[3, 2, null, 5, null, 1, 2], 'a'), (ARRAY[null, 1, 3, 5, 7, 9, 11], 'a'), (ARRAY[7, 3, 2, 6, 5, 4, 3], 'b'), (ARRAY[2, 6, 3, 5, null, 1, 6], 'b')", 4, """
                VALUES
                ROW(ROW(CAST(NULL AS ARRAY(INTEGER)), CAST(NULL AS ARRAY(INTEGER)), CAST(NULL AS BIGINT))),
                ROW(ROW(CAST(NULL AS ARRAY(INTEGER)), CAST(NULL AS ARRAY(INTEGER)), CAST(NULL AS BIGINT)))
                """);
        testPartitionsTableColumnTypes("MAP(INTEGER, INTEGER)", "VALUES (MAP(ARRAY[1,3], ARRAY[2,4]), 'a'), (MAP(ARRAY[1,2], ARRAY[3,4]), 'a'), (MAP(ARRAY[8,3], ARRAY[7,4]), 'b'), (MAP(ARRAY[1,5], ARRAY[2,7]), 'b')", 4, """
                VALUES
                ROW(ROW(CAST(NULL AS MAP(INTEGER, INTEGER)), CAST(NULL AS MAP(INTEGER, INTEGER)), CAST(NULL AS BIGINT))),
                ROW(ROW(CAST(NULL AS MAP(INTEGER, INTEGER)), CAST(NULL AS MAP(INTEGER, INTEGER)), CAST(NULL AS BIGINT)))
                """);
        testPartitionsTableColumnTypes("ROW(row_integer_1 INTEGER, row_integer_2 INTEGER)", "VALUES (ROW(1,3), 'a'), (ROW(1,2), 'a'), (ROW(8,3), 'b'), (ROW(1,5), 'b')", 4, """
                VALUES
                ROW(ROW(CAST(NULL AS ROW(row_integer_1 INTEGER, row_integer_2 INTEGER)), CAST(NULL AS ROW(row_integer_1 INTEGER, row_integer_2 INTEGER)), CAST(NULL AS BIGINT))),
                ROW(ROW(CAST(NULL AS ROW(row_integer_1 INTEGER, row_integer_2 INTEGER)), CAST(NULL AS ROW(row_integer_1 INTEGER, row_integer_2 INTEGER)), CAST(NULL AS BIGINT)))
                """);
    }

    private void testPartitionsTableColumnTypes(String type, @Language("SQL") String insertIntoValues, int insertIntoValuesCount, @Language("SQL") String expectedDataColumn)
    {
        String tableName = "test_partitions_table_data_column_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_nonpartition " + type + ", _partition VARCHAR) WITH (partitioned_by = ARRAY['_partition'])");
            assertUpdate("INSERT INTO " + tableName + " " + insertIntoValues, insertIntoValuesCount);
            assertThat(query("SELECT CAST(data._nonpartition AS ROW(" + type + "," + type + ", BIGINT)) FROM \"" + tableName + "$partitions\"")).matches(expectedDataColumn);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionsTableUnpartitioned()
    {
        String tableName = "test_partitions_table_unpartitioned_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(_bigint BIGINT, _date DATE)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 6");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$partitions\"", "VALUES 0");
            assertQueryReturnsEmptyResult("SELECT * FROM \"" + tableName + "$partitions\"");

            assertQuery("SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                    """
                            VALUES
                            ('file_count', 'bigint', '', ''),
                            ('total_size', 'bigint', '', ''),
                            ('data', 'row(_bigint row(min bigint, max bigint, null_count bigint), _date row(min date, max date, null_count bigint))', '', '')
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
